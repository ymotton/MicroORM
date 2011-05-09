using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using MicroORM.Data;

namespace MicroORM.Consumer
{
    class Program
    {
        static void Main(string[] args)
        {
            using (var connection = new MicroOrmConnection(new SqlConnection(@"Data Source=.\SQLEXPRESS; Initial Catalog=BCN_WK_00_Northwind; Integrated Security=SSPI;")))
            {
                connection.Open();

                TestRun(connection);

                connection.Entity<Customer>()
                    .MapsToTable("Customers")
                    .HasKey(c => c.CustomerID)
                    .HasMany(c => c.Orders)
                    .WithOne(o => o.Customer)
                    .HasForeignKey(o => o.CustomerID);

                connection.Entity<Order>()
                    .MapsToTable("Orders")
                    .HasKey(o => o.OrderID)
                    .HasOne(o => o.Customer)
                    .WithMany(c => c.Orders)
                    .HasForeignKey(o => o.CustomerID);

                var customers =
                    connection.Query<Customer>(
                        "SELECT * FROM Customers LEFT JOIN Orders ON Customers.CustomerID = Orders.CustomerID").ToList();
                Console.WriteLine(customers);
            }
        }

        /// <summary>
        /// 5192 ms
        /// 4735 ms
        /// 4703 ms
        /// 4345 ms
        /// </summary>
        /// <param name="connection"></param>
        static void TestRun(MicroOrmConnection connection)
        {
            Benchmark(
                () => connection.Query<Customer>("SELECT * FROM Customers WHERE CustomerID != @CustomerID", new { CustomerID = "123456" }).ToList()
            );
            Benchmark(
                () => connection.Query("SELECT * FROM Customers WHERE CustomerID != @CustomerID", "@CustomerID", "123456").ToList()
            );

            Benchmark(
                () => connection.Query<Customer>("SELECT * FROM Customers").ToList()
            );
            Benchmark(
                () => connection.Query("SELECT * FROM Customers").ToList()
            );
        }

        static void Benchmark(Action action, int iterations = 1000)
        {
            Stopwatch sw = Stopwatch.StartNew();
            for (int i = 0; i < iterations; i++)
            {
                action();
            }
            sw.Stop();
            Console.WriteLine("{0} ms", sw.ElapsedMilliseconds);
        }
    }

    public class Customer
    {
        public string CustomerID { get; set; }
        public string CompanyName { get; set; }
        public string ContactName { get; set; }
        public string ContactTitle { get; set; }
        public string Address { get; set; }
        public string Country { get; set; }
        public string Region { get; set; }
        public string City { get; set; }
        public string PostalCode { get; set; }
        public string Phone { get; set; }
        public string Fax { get; set; }
        public string Sex { get; set; }

        public ICollection<Order> Orders { get; set; }
    }

    public class Order
    {
        public string OrderID { get; set; }
        public string CustomerID { get; set; }

        public Customer Customer { get; set; }
    }
}