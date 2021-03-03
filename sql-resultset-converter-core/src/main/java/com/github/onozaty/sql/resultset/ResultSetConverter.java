package com.github.onozaty.sql.resultset;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * {@link ResultSet}の変換処理です。
 * @author onozaty
 */
public class ResultSetConverter {

    private final ResultSet resultSet;
    private final List<Column<?>> columns;

    private ResultSetConverter(ResultSet resultSet, List<Column<?>> columns) throws SQLException {

        this.resultSet = resultSet;
        this.columns = columns;
    }

    /**
     * 変換します。
     * @param connection DBコネクション
     * @param sql SQL
     * @param outputDestination 出力先
     * @throws SQLException
     * @throws IOException
     */
    public static void convert(Connection connection, String sql, OutputDestination outputDestination)
            throws SQLException, IOException {

        try (Statement statement = connection.createStatement()) {

            try (ResultSet resultSet = statement.executeQuery(sql)) {

                from(resultSet).to(outputDestination);
            }
        }
    }

    /**
     * インスタンスを生成します。
     * @param resultSet 結果セット
     * @param columns カラム一覧
     * @return インスタンス
     * @throws SQLException
     */
    public static ResultSetConverter from(ResultSet resultSet, List<Column<?>> columns) throws SQLException {
        return new ResultSetConverter(resultSet, columns);
    }

    /**
     * インスタンスを生成します。
     * @param resultSet 結果セット
     * @return インスタンス
     * @throws SQLException
     */
    public static ResultSetConverter from(ResultSet resultSet) throws SQLException {
        return from(resultSet, ColumnFactory.createColumns(resultSet.getMetaData()));
    }

    /**
     * 変換結果を出力します。
     * @param outputDestination 出力先
     * @throws SQLException
     * @throws IOException
     */
    public void to(OutputDestination outputDestination) throws SQLException, IOException {

        outputDestination.prepare(columns);

        while (resultSet.next()) {

            for (Column<?> column : columns) {
                outputDestination.output(
                        column,
                        column.getRetriever().retrieve(resultSet));
            }

            outputDestination.endRecord();

        }
    }
}
