namespace Yinyang.Utilities.SqlServer.Samples
{
    public class SqlServer
    {
        /// <summary>
        ///     ConnectionString
        /// </summary>
        /// <remarks>
        ///     接続文字列
        /// </remarks>
        private const string ConnectionString =
            "Data Source=db.local;Persist Security Info=True;User ID=xxxx;Password=yyyy;";


        /// <summary>
        ///     Constructor
        /// </summary>
        /// <remarks>
        ///     ConnectionString設定後、省略可能
        /// </remarks>
        public SqlServer() =>
            // Set default connection string
            Utilities.SqlServer.SqlServer.ConnectionString = ConnectionString;

        /// <summary>
        ///     Select Sample
        /// </summary>
        public void Select()
        {
            // Use default connection string
            using (var db = new Utilities.SqlServer.SqlServer())
            {
                db.Open();
                db.CommandText = "select * from test;";
                var result = db.ExecuteReader<Entity>();
                db.Close();
            }

            using (var db = new Utilities.SqlServer.SqlServer(ConnectionString))
            {
                db.Open();
                db.CommandText = "select * from test;";
                var result = db.ExecuteReader<Entity>();
                db.Close();
            }
        }

        /// <summary>
        ///     Insert Sample
        /// </summary>
        public void Insert()
        {
            using var db = new Utilities.SqlServer.SqlServer(ConnectionString);

            db.Open();

            db.BeginTransaction();

            db.CommandText = "INSERT INTO test2 VALUES(@id, @value)";
            db.AddParameter("@id", 1);
            db.AddParameter("@value", "あいうえお");
            if (1 != db.ExecuteNonQuery())
            {
                db.Rollback();
                return;
            }

            db.Refresh();

            db.CommandText = "select * from test2 where \"id\" = @id;";
            db.AddParameter("@id", 1);
            var result = db.ExecuteReaderFirst<Entity>();

            if (null == result)
            {
                db.Rollback();
                return;
            }

            db.Commit();

            db.Close();
        }
    }
}
