using Azure;
using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace LiteDB.AzureBlob
{
    /// <summary>
    /// Use Azure Page Blob as storage backend for LiteDB 5
    /// </summary>
    public class AzurePageBlobStreamNew : Stream
    {
        // configurations
        public static bool WriteDebugLogs = false;
        public static int PageSize = 1024 * 8;  // Default PageSize of LiteDB 5
        public static int Pages = 8;
        public const string DefaultContainerName = "litedbs";
        public static long DefaultStreamSize = 1024 * 1024 * 10; //1G
        public static AccessTier DefaultBlobTier = AccessTier.P4;

        private const string MetadataLengthKey = "STREAM_LENGTH";

        readonly PageBlobClient Blob;
        readonly ConcurrentDictionary<long, byte[]> Cache = new ConcurrentDictionary<long, byte[]>();
        const int NumberOfLocks = 111;  // should be more than enough
        readonly object[] Locks = new object[NumberOfLocks];
        readonly ConcurrentDictionary<long, byte[]> Pending = new ConcurrentDictionary<long, byte[]>();
        readonly BlockingCollection<byte[]> Buffers = new BlockingCollection<byte[]>();
        readonly ConcurrentDictionary<long, Lazy<ReaderWriterLockSlim>> locks = new ConcurrentDictionary<long, Lazy<ReaderWriterLockSlim>>();
        readonly ConcurrentDictionary<long, Task<BlobDownloadResult>> ongoingDownloads = new ConcurrentDictionary<long, Task<BlobDownloadResult>>();
        long LazyLength = 0;

        private IDictionary<string, string> metaData;
        private readonly ILogger logger;

        public AzurePageBlobStreamNew(string connString, string databaseName, ILogger logger = null)
            : this(GetBlobReference(connString, databaseName), databaseName)
        {
            this.logger = logger;
        }

        public AzurePageBlobStreamNew(string storageAccount, string containerName, string databaseName, ILogger logger = null)
            : this(GetBlobReferenceFromStorageAccount(storageAccount, containerName, databaseName), databaseName)
        {
            this.logger = logger;
        }

        private AzurePageBlobStreamNew(PageBlobClient blob, string databaseName)
        {
            if (!blob.Exists().Value)
            {
                if (WriteDebugLogs)
                    Log($"Creating new page blob file {databaseName}", LogLevel.Debug);
                var contentInfo = blob.Create(DefaultStreamSize);
                //blob.SetAccessTier(DefaultBlobTier);
            }
            Blob = blob;
            for (var i = 0; i < NumberOfLocks; i++)
                Locks[i] = new object();
            for (var i = 0; i < Math.Max(Environment.ProcessorCount * 2, 10); i++)
                Buffers.Add(new byte[PageSize * Pages]);
            if (Length != 0)
                ReadAhead(0);
        }

        private static PageBlobClient GetBlobReference(string connString, string databaseName, string containerName = DefaultContainerName)
        {
            var client = new BlobServiceClient(connString);
            return GetBlobClient(client, containerName, databaseName);
        }

        private static PageBlobClient GetBlobReferenceFromStorageAccount(string storageAccount, string containerName, string databaseName)
        {
            var client = new BlobServiceClient(new Uri($"https://{storageAccount}.blob.core.windows.net"), new DefaultAzureCredential());
            return GetBlobClient(client, containerName, databaseName);
        }

        private static PageBlobClient GetBlobClient(BlobServiceClient client, string containerName, string databaseName)
        {
            var containerClient = client.GetBlobContainerClient(containerName);
            containerClient.CreateIfNotExists();
            var blob = containerClient.GetPageBlobClient(databaseName);
            return blob;
        }

        public override bool CanRead => true;

        public override bool CanSeek => false;

        public override bool CanWrite => true;

        private void SetLengthInternal(long newLength)
        {
            _Length = newLength;
            if (WriteDebugLogs)
                Log($"SetLength = {newLength / PageSize}", LogLevel.Debug);
            try
            {
                if (metaData == null)
                {
                    Log($"Metadata is not yet known, downloading..", LogLevel.Warning);
                    metaData = Blob.GetProperties().Value.Metadata;
                }
                metaData[MetadataLengthKey] = newLength.ToString();
                Blob.SetMetadataAsync(metaData); // .Wait();
            }
            catch (RequestFailedException e)
            {
                Log($"Unable to update metadata for {Blob.Name}: {e.Message}", LogLevel.Error);
            }
        }

        long? _Length = null;
        public override long Length
        {
            get
            {
                if (!_Length.HasValue)
                {
                    metaData = Blob.GetProperties().Value.Metadata;
                    if (!metaData.TryGetValue(MetadataLengthKey, out string value) || !long.TryParse(value, out long realLength))
                    {
                        SetLengthInternal(0);
                        _Length = 0;
                        return 0;
                    }
                    if (realLength % PageSize != 0)
                        throw new NotImplementedException("file size is invalid!");
                    if (WriteDebugLogs)
                        Log($"GetLength = {realLength / PageSize}", LogLevel.Debug);
                    _Length = realLength;
                }
                return _Length.Value;
            }
        }

        public override long Position { get; set; }

        object GetLock(long position)
        {
            return Locks[(position / PageSize) % NumberOfLocks];
        }

        object[] GetLocks(IEnumerable<long> ps)
        {
            return ps.Select(t => (t / PageSize) % NumberOfLocks).Distinct().Select(t => Locks[t]).ToArray();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            if (offset % PageSize != 0)
                throw new NotImplementedException("invalid offset");
            if (count != PageSize)
                throw new NotImplementedException($"Read count is not {PageSize}; it is possible that you are not using LiteDB 5");

            var position = Position;

            byte[] cached;
            lock (GetLock(position))
            {
                if (!Cache.TryGetValue(position, out cached))
                    cached = null;
                else
                    Buffer.BlockCopy(cached, 0, buffer, offset, count);
                Position += count;
            }
            if (cached == null)
            {
                ReadAhead(position, buffer, offset);
                return count;
            }
            else
            {
                Task.Run(() =>
                {
                    for (var i = 1; i < Pages * 2; i++)
                    {
                        var off = position + PageSize * i;
                        if (Cache.ContainsKey(off) == false)
                        {
                            ReadAhead(off);
                            break;
                        }
                    }
                });
                return count;
            }
        }

        void ReadAhead(long position, byte[] bufToReturn = null, int offset = 0)
        {
            var buf = Buffers.Take();

            byte[] CachePage(int i)
            {
                var offsetBuf = i * PageSize;
                var offsetStream = position + offsetBuf;
                lock (GetLock(offsetStream))
                {
                    if (Cache.TryGetValue(offsetStream, out byte[] tmp))
                        return tmp;

                    tmp = new byte[PageSize];
                    Buffer.BlockCopy(buf, offsetBuf, tmp, 0, PageSize);
                    Cache[offsetStream] = tmp;
                    return tmp;
                }
            }

            if (bufToReturn != null)
            {
                var data = DownloadContent(buf, position);
                if (data != null)
                {
                    //var res = Blob.DownloadContent(new BlobDownloadOptions { Range = new HttpRange(position, buf.Length) });
                    //res.Value.Content.ToMemory().CopyTo(buf);
                    data.ToMemory().CopyTo(buf);
                    Buffer.BlockCopy(CachePage(0), 0, bufToReturn, offset, PageSize);
                    Task.Run(() =>
                    {
                        for (var i = 1; i < Pages; i++)
                            CachePage(i);
                        Buffers.Add(buf);
                    });
                }
            }
            else
            {
                Task.Run(() =>
                {
                    var data = DownloadContent(buf, position);
                    if (data != null)
                    {
                        data.ToMemory().CopyTo(buf);
                        //var res = Blob.DownloadContent(new BlobDownloadOptions { Range = new HttpRange(position, buf.Length) });
                        //res.Value.Content.ToMemory().CopyTo(buf);
                        for (var i = 0; i < Pages; i++)
                            CachePage(i);
                        Buffers.Add(buf);
                    }
                });
            }
        }

        private BinaryData DownloadContent(byte[] buf, long position)
        {
            var myLock = locks.GetOrAdd(position, LockFactory);
            Task<BlobDownloadResult> downloadJob = null;
            bool lockEntered = false, jobAdded = false;
            try
            {
                lockEntered = myLock.Value.TryEnterWriteLock(-1);
                if (lockEntered)
                {
                    if (!ongoingDownloads.TryGetValue(position, out downloadJob))
                    {
                        Log($"Starting new download job for position {position}", LogLevel.Debug);
                        downloadJob = DownloadBlock(position, buf.Length);
                        jobAdded = ongoingDownloads.TryAdd(position, downloadJob);
                    }
                    //else
                    //    Log($"There's already an ongoing download for position {position}", LogLevel.Debug);
                }
            }
            catch (Exception e)
            {
                Log($"Error while trying to download block at position {position}: {e.Message}", LogLevel.Error);
                return null;
            }
            finally
            {
                if (lockEntered && myLock.Value.IsWriteLockHeld)
                    myLock.Value.ExitWriteLock();
            }
            try
            {
                downloadJob.Wait();
                if (downloadJob.IsCompleted)
                {
                    return downloadJob.Result.Content;
                }
                else if (downloadJob.IsCanceled)
                {
                    Log($"Download for position {position} was canceled", LogLevel.Warning);
                }
                else if (downloadJob.IsFaulted)
                {
                    Log($"Download for position {position} failed: {downloadJob.Exception?.Message}", LogLevel.Error);
                }
            }
            catch (Exception e)
            {
                Log($"Error while waiting for download completion for position {position}: {e.Message}", LogLevel.Error);
                return null;
            }
            finally
            {
                if (jobAdded)
                    _ = RemoveFromOngoingDownloads(myLock.Value, position);
                //ongoingDownloads.TryRemove(position, out _);
            }
            return null;
        }

        private async Task<BlobDownloadResult> DownloadBlock(long position, int length)
        {
            if (WriteDebugLogs)
                Log($"Downloading from {position} length {length}", LogLevel.Debug);
            try
            {
                var res = await Blob.DownloadContentAsync(new BlobDownloadOptions { Range = new HttpRange(position, length) });
                metaData = res.Value.Details.Metadata;
                Log($"Downloading from {position} length {length} complete", LogLevel.Debug);
                return res.Value;
            }
            catch (Exception e)
            {
                Log($"Unable to download block at position {position}: {e.Message}", LogLevel.Error);
                return null;
            }
        }

        private async Task RemoveFromOngoingDownloads(ReaderWriterLockSlim myLock, long position)
        {
            bool lockEntered = false;
            try
            {
                await Task.Delay(5000).ConfigureAwait(false);
            }
            catch (Exception) { }
            try
            {
                lockEntered = myLock.TryEnterWriteLock(5000);
                if (!lockEntered)
                {
                    Log($"Unable to enter write lock for removal of number lock of position {position}", LogLevel.Error);
                }
            }
            catch (Exception e)
            {
                Log($"Unable to acquire lock for position {position}, in download postprocessing: {e.Message}", LogLevel.Error);
            }
            finally
            {
                if (!ongoingDownloads.TryRemove(position, out _))
                    Log($"Unable to remove ongoing download for position {position}", LogLevel.Error);
                try
                {
                    if (lockEntered && myLock.IsWriteLockHeld)
                    {
                        myLock.ExitWriteLock();
                    }
                }
                catch (Exception e)
                {
                    Log($"Unable to exit write lock for download of position {position}: {e.Message}", LogLevel.Error);
                }
            }
        }


        public override long Seek(long offset, SeekOrigin origin)
        {
            throw new NotImplementedException();
        }

        public override void SetLength(long value)
        {
            Debug.Assert(value % PageSize == 0);
            SetLengthInternal(value);
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            if (offset != 0)
                throw new NotImplementedException("write offset is not 0; it is possible that your are not using LiteDB 5");
            if (count != PageSize)
                throw new NotImplementedException($"write count is not {PageSize}; it is possible that you are not using LiteDB 5");

            var position = Position;
            lock (GetLock(position))
            {
                if (!Cache.TryGetValue(position, out byte[] cached))
                    Cache.TryAdd(position, cached = new byte[PageSize]);
                Buffer.BlockCopy(buffer, 0, cached, 0, count);
                lock (Pending)
                {
                    Pending[position] = cached;
                }
                Position += count;
                if (Position > LazyLength)
                    LazyLength = Position;
            }
        }

        public override void Flush()
        {
            if (Pending.Count == 0)
                return;

            if (WriteDebugLogs)
                Log($"Flush {string.Join(",", Pending.Select(t => t.Key / PageSize))}", LogLevel.Debug);

            Queue<KeyValuePair<long, byte[]>> queue;
            lock (Pending)
            {
                queue = new Queue<KeyValuePair<long, byte[]>>(Pending.OrderByDescending(t => t.Key));
                Pending.Clear();
            }

            var tasks = new List<Task>();
            var locks = GetLocks(queue.Select(t => t.Key));
            foreach (var m in locks)
                Monitor.Enter(m);

            try
            {
                while (true)
                {
                    var batch = new Stack<KeyValuePair<long, byte[]>>();
                    while (batch.Count < Pages && queue.Count > 0)
                    {
                        var next = queue.Peek();
                        if (batch.Count == 0 || batch.Peek().Key - PageSize == next.Key)
                        {
                            queue.Dequeue();
                            batch.Push(next);
                        }
                        else
                            break;
                    }
                    if (batch.Count == 0)
                        break;
                    else if (batch.Count == 1)
                    {
                        var task = Task.Run(() =>
                        {
                            var p = batch.Peek();
                            try
                            {
                                using (var ms = new MemoryStream(p.Value))
                                {
                                    if (WriteDebugLogs)
                                        Log($"WriteOne @{p.Key / PageSize}", LogLevel.Debug);
                                    Blob.UploadPages(ms, p.Key);
                                }
                            }
                            catch (RequestFailedException e)
                            {
                                Log($"Error while writing single page at {p.Key / PageSize}: {e.Message}", LogLevel.Error);
                            }
                        });
                        tasks.Add(task);
                    }
                    else
                    {
                        var task = Task.Run(() =>
                        {
                            var buf = Buffers.Take();
                            long offsetStart = 0;
                            int offsetWithinBuf = 0;
                            long offsetLast = -1;
                            var cnt = batch.Count;
                            while (batch.Count > 0)
                            {
                                var p = batch.Pop();
                                if (offsetLast < 0)
                                {
                                    offsetStart = offsetLast = p.Key;
                                }
                                else
                                {
                                    Debug.Assert(offsetLast + PageSize == p.Key);
                                    offsetLast = p.Key;
                                }
                                Buffer.BlockCopy(p.Value, 0, buf, offsetWithinBuf, PageSize);
                                offsetWithinBuf += PageSize;
                            }
                            if (WriteDebugLogs)
                                Log($"WriteBatch @{offsetStart / PageSize} #{cnt}", LogLevel.Debug);
                            try
                            {
                                using (var ms = new MemoryStream(buf, 0, offsetWithinBuf))
                                {
                                    Blob.UploadPages(ms, offsetStart);
                                }   
                            }
                            catch (RequestFailedException e)
                            {
                                Log($"Error while writing batch at {offsetStart / PageSize}: {e.Message}", LogLevel.Error);
                            }
                            Buffers.Add(buf);
                        });
                        tasks.Add(task);
                    }
                }

                SetLengthInternal(LazyLength);
                Task.WaitAll(tasks.ToArray());
            }
            finally
            {
                foreach (var t in locks)
                    Monitor.Exit(t);
            }
        }

        public static void DropDatabase(string connString, string name)
        {
            Console.WriteLine($"Deleting {name}");
            var blob = GetBlobReference(connString, name);
            blob.DeleteIfExists();
        }

        public static void DropDatabase(string accountName, string containerName, string name)
        {
            Console.WriteLine($"Deleting {name}");
            var blob = GetBlobReferenceFromStorageAccount(accountName, containerName, name);
            blob.DeleteIfExists();
        }

        public static void Download(string connString, string dbName, string localFile)
        {
            Console.WriteLine($"Download {dbName} to {localFile}");
            using (var s = File.OpenWrite(localFile))
            using (var page = new AzurePageBlobStreamNew(connString, dbName))
            {
                page.Blob.DownloadTo(s);
            }
        }

        public static void Download(string accountName, string containerName, string dbName, string localFile)
        {
            Console.WriteLine($"Download {dbName} to {localFile}");
            using (var s = File.OpenWrite(localFile))
            using (var page = new AzurePageBlobStreamNew(accountName, containerName, dbName))
            {
                page.Blob.DownloadTo(s);
            }
        }

        private void Log(string message, LogLevel level)
        {
            if (logger != null)
                logger?.Log(level, message);
            Console.WriteLine($"{level}|{message}");
        }

        private static Lazy<ReaderWriterLockSlim> LockFactory(long position)
        {
            return new Lazy<ReaderWriterLockSlim>(() => new ReaderWriterLockSlim());
        }

        /*
        public static void Upload(string localFile, string connString, string dbName)
        {
            Console.WriteLine("Upload " + localFile);
            using (var r = File.OpenRead(localFile))
            {
                Debug.Assert(r.Length % PageSize == 0);
                Console.WriteLine(r.Length);
                var buf = new byte[1024 * 1024];
                using (var stream = new AzurePageBlobStream(connString, dbName))
                {
                    int cnt;
                    while ((cnt = r.Read(buf, 0, buf.Length)) > 0)
                        stream.Write(buf, 0, cnt);
                    stream.SetLengthInternal(r.Length);
                }
            }
        }
        */

        private class DownloadIdentifier
        {
            internal long Position { get; set; }
        }
    }
}
