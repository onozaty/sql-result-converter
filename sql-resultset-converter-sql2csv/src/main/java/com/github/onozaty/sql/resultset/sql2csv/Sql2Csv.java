package com.github.onozaty.sql.resultset.sql2csv;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import com.github.onozaty.sql.resultset.core.CsvDestination;
import com.github.onozaty.sql.resultset.core.ResultSetConverter;

import lombok.Value;

/**
 * SQL結果からCSVへの変換を行うクラスです。
 * @author onozaty
 */
@Value
public class Sql2Csv {

    private DatabaseConnectionSettings databaseConnectionSettings;

    /**
     * メイン
     * @param args 引数
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        CommandLineParser parser = new DefaultParser();

        Options options = new Options();
        options.addOption(
                Option.builder("c")
                        .longOpt("conn")
                        .desc("Database connection setting file")
                        .hasArg()
                        .argName("conn.json")
                        .required()
                        .build());
        options.addOption(
                Option.builder("s")
                        .longOpt("sql")
                        .desc("SQL file")
                        .hasArg()
                        .argName("query.sql")
                        .required()
                        .build());
        options.addOption(
                Option.builder("o")
                        .longOpt("output")
                        .desc("Output CSV file")
                        .hasArg()
                        .argName("output.csv")
                        .required()
                        .build());

        try {
            CommandLine line = parser.parse(options, args);

            Path connectionSettingFilePath = Paths.get(line.getOptionValue("c"));
            Path sqlFilePath = Paths.get(line.getOptionValue("s"));
            Path outputFilePath = Paths.get(line.getOptionValue("o"));

            DatabaseConnectionSettings connectionSettings =
                    DatabaseConnectionSettings.of(connectionSettingFilePath);
            String sql = new String(Files.readAllBytes(sqlFilePath), StandardCharsets.UTF_8);

            new Sql2Csv(connectionSettings).convert(sql, outputFilePath);

        } catch (ParseException e) {
            System.out.println("Unexpected exception:" + e.getMessage());
            System.out.println();

            printUsage(options);
            return;
        }
    }

    private static void printUsage(Options options) {
        HelpFormatter help = new HelpFormatter();
        help.setWidth(200);
        help.setOptionComparator(null); // 順番を変えない

        // ヘルプを出力
        help.printHelp("java -jar sql2csv.jar", options, true);
        System.exit(1);
    }

    /**
     * SQL結果からCSVへ変換します。
     * @param sql SQL
     * @param outputFilePath 出力ファイルパス
     * @return 出力件数
     * @throws SQLException
     * @throws IOException
     */
    public int convert(String sql, Path outputFilePath)
            throws SQLException, IOException {

        try (
                Connection connection = databaseConnectionSettings.getConnection();
                Writer writer = Files.newBufferedWriter(outputFilePath, StandardCharsets.UTF_8);
                CsvDestination csvDestination = new CsvDestination(writer)) {

            return ResultSetConverter.convert(connection, sql, csvDestination);
        }
    }

}
