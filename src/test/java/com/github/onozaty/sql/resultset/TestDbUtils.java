package com.github.onozaty.sql.resultset;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * データベースに関するテスト用のユーティリティクラスです。
 * @author onozaty
 */
public class TestDbUtils {

    private static final String DATABASE_URL = "jdbc:h2:mem:";
    private static final String DATABASE_USER = "";
    private static final String DATABASE_PASSWORD = "";

    /**
     * DBコネクションを取得します。
     * @return DBコネクション
     * @throws SQLException
     */
    public static Connection getConnection() throws SQLException {

        return DriverManager.getConnection(
                DATABASE_URL,
                DATABASE_USER,
                DATABASE_PASSWORD);
    }

    /**
     * テーブルを準備します。
     * @param connection DBコネクション
     * @throws SQLException
     */
    public static void setupTable(Connection connection) throws SQLException {

        try (Statement statement = connection.createStatement()) {

            // http://www.h2database.com/html/datatypes.html
            statement.executeUpdate(
                    "DROP TABLE IF EXISTS rows;"
                            + "CREATE TEMPORARY TABLE rows ("
                            + "column_int INT, "
                            + "column_boolean BOOLEAN, "
                            + "column_tinyint TINYINT, "
                            + "column_smallint SMALLINT, "
                            + "column_bigint BIGINT, "
                            + "column_decimal DECIMAL(20, 3), "
                            + "column_double DOUBLE, "
                            + "column_real REAL, "
                            + "column_time TIME,"
                            + "column_time_tz TIME WITH TIME ZONE,"
                            + "column_date DATE, "
                            + "column_timestamp TIMESTAMP, "
                            + "column_timestamp_tz TIMESTAMP WITH TIME ZONE, "
                            + "column_varchar VARCHAR(100),"
                            + "column_array ARRAY"
                            + ")");

            statement.executeUpdate(
                    "INSERT INTO rows VALUES ("
                            + "1, " // INT
                            + "TRUE, " // BOOLEAN, "
                            + "-1, " // TINYINT
                            + "100, " // SMALLINT
                            + "200000, " // BIGINT
                            + "1234.123, " // DECIMAL
                            + "1.1, " // DOUBLE
                            + "2.2, " // REAL
                            + "'23:59:59', " // TIME
                            + "'23:59:59+01', " // TIME WITH TIME ZONE
                            + "'2004-12-31'," // DATE
                            + "'1999-01-31 10:00:00', " // TIMESTAMP
                            + "'2005-12-31 23:59:59-10:00', " // TIMESTAMP WITH TIME ZONE
                            + "'xxxxxxx', " // VARCHAR(100)
                            + "ARRAY[1, 2]" // ARRAY
                            + ")");

            statement.executeUpdate(
                    "INSERT INTO rows VALUES ("
                            + "2, " // INT
                            + "FALSE, " // BOOLEAN, "
                            + "123, " // TINYINT
                            + "NULL, " // SMALLINT
                            + "-123456, " // BIGINT
                            + "1.0, " // DECIMAL
                            + "0.001, " // DOUBLE
                            + "-10.0, " // REAL
                            + "'00:00:00', " // TIME
                            + "'12:00:01+09', " // TIME WITH TIME ZONE
                            + "'2021-01-01'," // DATE
                            + "'2021-12-24 20:00:00.12345', " // TIMESTAMP
                            + "'2020-02-01 23:59:59+00:00', " // TIMESTAMP WITH TIME ZONE
                            + "'', " // VARCHAR(100)
                            + "ARRAY['a']" // ARRAY
                            + ")");
        }
    }
}
