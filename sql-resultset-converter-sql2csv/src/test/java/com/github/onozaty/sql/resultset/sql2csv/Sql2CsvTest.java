package com.github.onozaty.sql.resultset.sql2csv;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Test;

/**
 * {@link Sql2Csv}のテストクラスです。
 * @author onozaty
 */
public class Sql2CsvTest {

    private DatabaseConnectionSettings databaseConnectionSettings =
            new DatabaseConnectionSettings("jdbc:h2:./testdb/test", "", "");

    /**
     * {@link Sql2Csv#convert(String, Path)}のテストです。
     * @throws SQLException
     * @throws IOException
     * @throws URISyntaxException
     */
    @Test
    public void convert() throws SQLException, IOException, URISyntaxException {

        setupTable();

        Path tempFilePath = Files.createTempFile(this.getClass().getSimpleName(), ".csv");

        try {
            Sql2Csv sql2Csv = new Sql2Csv(databaseConnectionSettings);

            int count = sql2Csv.convert("SELECT * FROM users ORDER BY user_id", tempFilePath);

            assertThat(count).isEqualTo(2);
            assertThat(tempFilePath)
                    .hasSameBinaryContentAs(getResourcePath("convert-expected.csv"));

        } finally {
            Files.delete(tempFilePath);
        }
    }

    /**
     * {@link Sql2Csv#convert(String, Path)}のテストです。
     * @throws SQLException
     * @throws IOException
     * @throws URISyntaxException
     */
    @Test
    public void convert_カラム名変更() throws SQLException, IOException, URISyntaxException {

        setupTable();

        Path tempFilePath = Files.createTempFile(this.getClass().getSimpleName(), ".csv");

        try {
            Sql2Csv sql2Csv = new Sql2Csv(databaseConnectionSettings);

            int count = sql2Csv.convert("SELECT user_id AS 番号, name AS 名前 FROM users ORDER BY user_id", tempFilePath);

            assertThat(count).isEqualTo(2);
            assertThat(tempFilePath)
                    .hasSameBinaryContentAs(getResourcePath("convert_カラム名変更-expected.csv"));

        } finally {
            Files.delete(tempFilePath);
        }
    }

    private Path getResourcePath(String fileName) throws URISyntaxException {

        return Paths.get(this.getClass().getResource(fileName).toURI());
    }

    /**
     * テーブルを準備します。
     * @throws SQLException
     */
    private void setupTable() throws SQLException {

        try (Connection connection = databaseConnectionSettings.getConnection();
                Statement statement = connection.createStatement()) {

            // http://www.h2database.com/html/datatypes.html
            statement.executeUpdate(
                    "DROP TABLE IF EXISTS users;"
                            + "CREATE TABLE users ("
                            + "user_id INT, "
                            + "name VARCHAR(50), "
                            + "birthday DATE, "
                            + "create_at TIMESTAMP WITH TIME ZONE "
                            + ")");

            statement.executeUpdate(
                    "INSERT INTO users VALUES ("
                            + "1, "
                            + "'Hanako, Yamada', "
                            + "'1970-12-29', "
                            + "'2020-12-31 23:59:59-10:00' "
                            + ")");

            statement.executeUpdate(
                    "INSERT INTO users VALUES ("
                            + "2, "
                            + "'太郎', "
                            + "'1981-01-15', "
                            + "'2021-01-15 00:00:00+9:00' "
                            + ")");
        }
    }
}
