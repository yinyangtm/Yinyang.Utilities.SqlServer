# Yinyang Utilities SqlServer

[![Yinyang Utilities SqlServer](https://img.shields.io/nuget/v/Yinyang.Utilities.SqlServer.svg)](https://www.nuget.org/packages/Yinyang.Utilities.SqlServer/)

SqlServer Connection Utility for C#.

C#用SQL Server接続ユーティリティです。

---

## Getting started

Install Yinyang Utilities SqlServer nuget package.

NuGet パッケージ マネージャーからインストールしてください。

- [SqlServer](https://www.nuget.org/packages/Yinyang.Utilities.SqlServer/)

> ```powershell
> Install-Package Yinyang.Utilities.SqlServer
> ```

---

## Basic Usage

```c#
// Init
using var db = new SqlServer(ConnectionString);

// Database Open
db.Open();

// Transaction Start
db.BeginTransaction();

// SQL
db.CommandText = "INSERT INTO test2 VALUES(@id, @value)";

// Add Parameter
db.AddParameter("@id", 1);
db.AddParameter("@value", "abcdefg");

// Execute
if (1 != db.ExecuteNonQuery())
{
    // Transaction Rollback
    db.Rollback();
    return;
}

// Command and Parameter Reset
db.Refresh();

// SQL
db.CommandText = "select * from test2 where id = @id;";

// Add Parameter
db.AddParameter("@id", 1);

// Execute
var result = db.ExecuteReaderFirst<Entity>();

if (null == result)
{
    db.Rollback();
    return;
}

// Transaction Commit
db.Commit();

// Database Close
db.Close();


```

## Samples

See Sample project.
