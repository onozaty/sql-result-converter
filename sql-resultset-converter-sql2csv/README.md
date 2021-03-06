# sql2csv

Outputs SQL results as CSV.

## Usage

Java (JDK8 or higher) is required for execution.

Download the latest jar file (`sql2csv.jar`) from below.

* https://github.com/onozaty/sql-resultset-converter/releases/latest

Execute the application with the following command.

```
java -jar sql2csv.jar -c conn.json -q query.sql -o result.csv
```

```
usage: java -jar sql2csv.jar -c <conn.json> -q <query.sql> -o <output.csv>
 -c,--conn <conn.json>      Connection setting file
 -q,--query <query.sql>     SQL query file
 -o,--output <output.csv>   Output CSV file
 ```

In the Connection setting file, enter the Database connection information.

```json
{
  "url": "jdbc:postgresql://192.168.33.42/testdb",
  "user": "testuser",
  "password": "testpassword"
}
```

## Database

JDBC drivers for the following databases are included in the package.

* PostgreSQL
* MySQL
* SQL Server

When using other databases, please add the JDBC driver to your classpath.

```
java -classpath sql2csv.jar;hoge-jdbc.jar com.github.onozaty.sql.resultset.sql2csv.Sql2Csv -c conn.json -q query.sql -o result.csv
```
