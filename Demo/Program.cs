using LiteDB;
using LiteDB.AzureBlob;
using Microsoft.Extensions.Logging;
using System;
using System.IO;
using System.Linq;

namespace Demo
{
    class Program
    {
        static void Main(string[] args)
        {
            // TestLiteDBWithAzureBlockBlob();
            //TestLiteDBWithAzurePageBlob();
            TestLiteDBWithAzurePageBlobNew();
        }

        private static void TestLiteDBWithAzureBlockBlob()
        {
            var connectionString = "";  // <= put your Azure Premium Block Storage connection string here!!!
            if (string.IsNullOrWhiteSpace(connectionString))
                throw new NotImplementedException("Please provide connection string");
            var databaseName = "db1";

            // write
            AzureBlockBlobStream.WriteDebugLogs = true;
            using (var stream = new AzureBlockBlobStream(connectionString, databaseName))
                TestWriteDatabase(stream);

            // read
            using (var stream = new AzureBlockBlobStream(connectionString, databaseName))
                TestReadDatabase(stream);

            // clean up; you can checkout the files in azure portal before deleting the file
            Console.WriteLine($"[{DateTime.Now}] Drop database");
            AzureBlockBlobStream.DropDatabase(connectionString, databaseName);
            Console.WriteLine($"[{DateTime.Now}] Done");
        }

        private static void TestLiteDBWithAzurePageBlob()
        {
            var connectionString = "";  // <= put your Azure Page Blob connection string here!!
            if (string.IsNullOrWhiteSpace(connectionString))
                throw new NotImplementedException("Please provide connection string");
            var databaseName = "db1";

            // write
            AzurePageBlobStream.WriteDebugLogs = true;
            using (var stream = new AzurePageBlobStream(connectionString, databaseName))
                TestWriteDatabase(stream);

            AzurePageBlobStream.Download(connectionString, databaseName, @"c:\temp\testdb2.db");

            // read
            using (var stream = new AzurePageBlobStream(connectionString, databaseName))
                TestReadDatabase(stream);

            // clean up; you can checkout the files in azure portal before deleting the file
            Console.WriteLine($"[{DateTime.Now}] Drop database");
            AzurePageBlobStream.DropDatabase(connectionString, databaseName);
            Console.WriteLine($"[{DateTime.Now}] Done");
        }

        private static void TestLiteDBWithAzurePageBlobNew()
        {
            using ILoggerFactory factory = LoggerFactory.Create(builder => builder.AddConsole());
            ILogger logger = factory.CreateLogger<Program>();

            var databaseName = "db1";
            string accountName = "audmstorage";
            var containerName = AzurePageBlobStream.DefaultContainerName;

            // write
            AzurePageBlobStreamNew.WriteDebugLogs = true;
            using (var stream = new AzurePageBlobStreamNew(accountName, containerName, databaseName, logger))
            {
                TestWriteDatabase(stream);
                TestReadDatabase(stream);
            }

            AzurePageBlobStreamNew.Download(accountName, containerName, databaseName, @"c:\temp\testdb2.db");

            // read
            using (var stream = new AzurePageBlobStreamNew(accountName, containerName, databaseName, logger))
                TestReadDatabase(stream);

            // clean up; you can checkout the files in azure portal before deleting the file
            Console.WriteLine($"[{DateTime.Now}] Drop database");
            AzurePageBlobStreamNew.DropDatabase(accountName, containerName, databaseName);
            Console.WriteLine($"[{DateTime.Now}] Done");
        }

        private static void TestWriteDatabase(Stream stream)
        {
            Console.WriteLine($"[{DateTime.Now}] Start writing");
            using (var db = new LiteDatabase(stream))
            {
                var collection = db.GetCollection<Book>();
                //foreach (var i in Enumerable.Range(1, 1000))
                //{
                //    var blog = new Book
                //    {
                //        Id = i,
                //        Title = "fake title " + i,
                //        Author = "fake author " + i,
                //        Description = $"fake description {i} fake description end"
                //    };
                //    collection.Upsert(blog);
                //}
                ParallelEnumerable.Range(1, 1000)
                    .ForAll(i =>
                    {
                        var blog = new Book
                        {
                            Id = i,
                            Title = "fake title " + i,
                            Author = "fake author " + i,
                            Description = $"fake description {i} fake description end"
                        };
                        collection.Upsert(blog);
                    });
                db.Checkpoint(); // flush
            }
            Console.WriteLine($"[{DateTime.Now}] Finish writing");
        }

        private static void TestReadDatabase(Stream stream)
        {
            Console.WriteLine($"[{DateTime.Now}] Start reading");
            using (var db = new LiteDatabase(stream))
            {
                var collection = db.GetCollection<Book>();
                var allBooks = collection.FindAll().ToList();
                //foreach (var i in Enumerable.Range(1, 100))
                //{
                //    var id = i * 5;
                //    var blog = collection.FindById(id);
                //    if (blog == null)
                //        throw new NotImplementedException("Cannot find " + id);
                //    else
                //        Console.WriteLine($"{blog.Id}:{blog.Title}");
                //}
                ParallelEnumerable.Range(1, 100)
                    .ForAll(i =>
                    {
                        var id = i * 5;
                        var blog = collection.FindById(id);
                        //if (blog == null)
                        //    throw new NotImplementedException($"Cannot find {id}");
                        //else
                        //    Console.WriteLine($"{blog.Id}:{blog.Title}");
                    });
            }
            Console.WriteLine($"[{DateTime.Now}] Finish reading");
        }
    }

    public class Book
    {
        public int Id { get; set; }
        public string Title { get; set; }
        public string Description { get; set; }
        public string Author { get; set; }
    }
}
