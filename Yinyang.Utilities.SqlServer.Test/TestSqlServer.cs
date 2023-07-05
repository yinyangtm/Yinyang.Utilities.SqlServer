using System;
using System.Data;
using System.Linq;
using Microsoft.Extensions.Configuration;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Yinyang.Utilities.SqlServer.Test
{
    [TestClass]
    public class TestSqlServer
    {
        private readonly string _connectionString;

        private IConfiguration Configuration { get; }

        public TestSqlServer()
        {
            Configuration = new ConfigurationBuilder()
                .AddUserSecrets<TestSqlServer>()
                .AddEnvironmentVariables()
                .Build();

            _connectionString = Configuration["sqlserver"];
            if (string.IsNullOrEmpty(_connectionString))
            {
                throw new ArgumentException();
            }
        }

        [TestMethod]
        public void EasySelect()
        {
            SqlServer.ConnectionString = _connectionString;
            using (var SqlServer = new SqlServer(_connectionString))
            {
                var result = SqlServer.EasySelect<EntityTest>("select * from test where \"id\" = 1;").First();

                var answer = new EntityTest {id = 1, key = 1, value = "あいうえお"};
                Assert.AreEqual(answer.id, result.id);
                Assert.AreEqual(answer.key, result.key);
                Assert.AreEqual(answer.value, result.value);
                SqlServer.Close();
            }
        }

        [TestMethod]
        public void ExecuteReaderFirst()
        {
            using (var SqlServer = new SqlServer(_connectionString))
            {
                SqlServer.Open();
                SqlServer.CommandText = "select * from test where \"id\" = 1;";
                var result = SqlServer.ExecuteReaderFirst<EntityTest>();

                var answer = new EntityTest {id = 1, key = 1, value = "あいうえお"};
                Assert.AreEqual(answer.id, result.id);
                Assert.AreEqual(answer.key, result.key);
                Assert.AreEqual(answer.value, result.value);
                SqlServer.Close();
            }
        }

        [TestMethod]
        public void Select()
        {
            using (var SqlServer = new SqlServer(_connectionString))
            {
                SqlServer.Open();
                SqlServer.CommandText = "select * from test where \"id\" = 1;";
                var result = SqlServer.ExecuteReader<EntityTest>().First();

                var answer = new EntityTest {id = 1, key = 1, value = "あいうえお"};
                Assert.AreEqual(answer.id, result.id);
                Assert.AreEqual(answer.key, result.key);
                Assert.AreEqual(answer.value, result.value);
                SqlServer.Close();
            }
        }

        [TestMethod]
        public void SelectCount()
        {
            using (var SqlServer = new SqlServer(_connectionString))
            {
                SqlServer.Open();
                SqlServer.CommandText = "select count(*) from test where \"id\" = 1;";
                var result = SqlServer.ExecuteScalarToInt();

                Assert.AreEqual(1, result);
                SqlServer.Close();
            }
        }

        [TestMethod]
        public void StoredProcedure()
        {
            using (var SqlServer = new SqlServer(_connectionString))
            {
                SqlServer.Open();
                SqlServer.ChangeCommandType(CommandType.StoredProcedure);
                SqlServer.CommandText = "GetTestData";
                SqlServer.AddParameter("@id", 1);
                var result = SqlServer.ExecuteReaderFirst<EntityTest>();

                var answer = new EntityTest {id = 1, key = 1, value = "あいうえお"};
                Assert.AreEqual(answer.id, result.id);
                Assert.AreEqual(answer.key, result.key);
                Assert.AreEqual(answer.value, result.value);

                SqlServer.Close();
            }
        }

        [TestMethod]
        public void TableRowsCount()
        {
            using (var SqlServer = new SqlServer(_connectionString))
            {
                SqlServer.Open();
                Assert.AreEqual(1, SqlServer.TableRowsCount("test"));
                SqlServer.Close();
            }
        }
    }
}
