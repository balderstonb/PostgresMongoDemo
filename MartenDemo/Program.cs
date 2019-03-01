using Marten;
using System;
using System.Linq;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using System.Collections.Generic;
using System.IO;
using Microsoft.Extensions.Configuration;

namespace MartenDemo
{
    class Program
    {
        private enum Database
        {
            Postgres,
            Mongo
        }

        private static Database DbInUse = Database.Mongo;

        private static string PostgresConnStr;
        private static string MongoConnStr;

        static void Main(string[] args)
        {
            var builder = new ConfigurationBuilder().SetBasePath(Directory.GetCurrentDirectory()).AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);

            IConfigurationRoot configuration = builder.Build();

            PostgresConnStr = configuration.GetConnectionString("Postgres");
            MongoConnStr = configuration.GetConnectionString("Mongo");

            var user = new User { FirstName = "Han", LastName = "Solo" };

            AddUser(user);

            var user_from_db = Users.FirstOrDefault(f => f.FirstName == "Han");

            Console.WriteLine($"Using {DbInUse} {user_from_db.Id}: {user_from_db.FirstName}");
        }
        
        private static void AddUser(User user)
        {
            if (DbInUse == Database.Postgres)
            {
                var marten = DocumentStore.For(PostgresConnStr);

                using (var session = marten.LightweightSession())
                {
                    session.Store(user);

                    session.SaveChanges();
                }
            }
            else
            {
                var mongo = new MongoClient(MongoConnStr);

                var mongo_db = mongo.GetDatabase(MongoUrl.Create(MongoConnStr).DatabaseName);

                mongo_db.GetCollection<User>(nameof(User)).InsertOne(user);
            }
        }

        private static IEnumerable<User> Users
        {
            get
            {
                if (DbInUse == Database.Postgres)
                {
                    var marten = DocumentStore.For(PostgresConnStr);

                    using (var session = marten.OpenSession())
                    {
                        return session.Query<User>().AsEnumerable();
                    }
                }
                else
                {
                    var mongo = new MongoClient(MongoConnStr);

                    var mongo_db = mongo.GetDatabase(MongoUrl.Create(MongoConnStr).DatabaseName);

                    return mongo_db.GetCollection<User>(nameof(User)).AsQueryable().ToEnumerable();
                }
            }
        }

        private class User
        {
            [BsonId]
            public Guid Id { get; set; }
            public string FirstName { get; set; }
            public string LastName { get; set; }
        }
    }
}
