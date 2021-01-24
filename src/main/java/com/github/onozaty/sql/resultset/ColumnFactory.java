package com.github.onozaty.sql.resultset;

import static java.time.format.DateTimeFormatter.ofPattern;

import java.math.BigDecimal;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * {@link Column}の生成処理です。
 * @author onozaty
 */
public class ColumnFactory {

    /**
     * {@link ResultSetMetaData}の情報から{@link Column}一覧を生成します。
     * @param metaData メタデータ
     * @return カラム一覧
     * @throws SQLException
     */
    public static List<Column<?>> createColumns(ResultSetMetaData metaData) throws SQLException {

        List<Column<?>> columns = new ArrayList<>();

        for (int columnIndex = 1, columnCount = metaData.getColumnCount(); columnIndex <= columnCount; columnIndex++) {

            columns.add(createColumn(metaData, columnIndex));
        }

        return columns;
    }

    private static Column<?> createColumn(ResultSetMetaData metaData, int columnIndex) throws SQLException {

        String columnName = metaData.getColumnName(columnIndex);
        int columnType = metaData.getColumnType(columnIndex);

        switch (columnType) {

            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:

                return new Column<String>(
                        columnName,
                        ValueRetriever.newValueRetriever(columnIndex, String.class));

            case Types.INTEGER:

                return new Column<Integer>(
                        columnName,
                        ValueRetriever.newValueRetriever(columnIndex, Integer.class));

            case Types.TINYINT:

                return new Column<Byte>(
                        columnName,
                        ValueRetriever.newValueRetriever(columnIndex, Byte.class));

            case Types.SMALLINT:

                return new Column<Short>(
                        columnName,
                        ValueRetriever.newValueRetriever(columnIndex, Short.class));

            case Types.BIGINT:

                return new Column<Long>(
                        columnName,
                        ValueRetriever.newValueRetriever(columnIndex, Long.class));

            case Types.NUMERIC:
            case Types.DECIMAL:

                return new Column<BigDecimal>(
                        columnName,
                        ValueRetriever.newValueRetriever(columnIndex, BigDecimal.class));

            case Types.FLOAT:
            case Types.DOUBLE:

                return new Column<Double>(
                        columnName,
                        ValueRetriever.newValueRetriever(columnIndex, Double.class));

            case Types.REAL:

                return new Column<Float>(
                        columnName,
                        ValueRetriever.newValueRetriever(columnIndex, Float.class));

            case Types.BIT:
            case Types.BOOLEAN:

                return new Column<Boolean>(
                        columnName,
                        ValueRetriever.newValueRetriever(columnIndex, Boolean.class));

            case Types.TIME:

                return new Column<String>(
                        columnName,
                        ValueRetriever.newFormattedValueRetriever(
                                columnIndex, LocalTime.class, DefaultDateTimeFormat.TIME));

            case Types.TIME_WITH_TIMEZONE:

                return new Column<String>(
                        columnName,
                        ValueRetriever.newFormattedValueRetriever(
                                columnIndex, OffsetTime.class, DefaultDateTimeFormat.OFFSET_TIME));

            case Types.DATE:

                return new Column<String>(
                        columnName,
                        ValueRetriever.newFormattedValueRetriever(
                                columnIndex, LocalDate.class, DefaultDateTimeFormat.DATE));

            case Types.TIMESTAMP:

                return new Column<String>(
                        columnName,
                        ValueRetriever.newFormattedValueRetriever(
                                columnIndex, LocalDateTime.class, DefaultDateTimeFormat.DATE_TIME));

            case Types.TIMESTAMP_WITH_TIMEZONE:

                return new Column<String>(
                        columnName,
                        ValueRetriever.newFormattedValueRetriever(
                                columnIndex, LocalDateTime.class, DefaultDateTimeFormat.OFFSET_DATE_TIME));

            default:

                return new Column<Object>(
                        columnName,
                        ValueRetriever.newValueRetriever(columnIndex, Object.class));
        }
    }

    private static class DefaultDateTimeFormat {

        private static final DateTimeFormatter DATE_TIME = ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSS");
        private static final DateTimeFormatter OFFSET_DATE_TIME = ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSSSSSxxxxx");
        private static final DateTimeFormatter DATE = ofPattern("uuuu-MM-dd");
        private static final DateTimeFormatter TIME = ofPattern("HH:mm:ss.SSS");
        private static final DateTimeFormatter OFFSET_TIME = ofPattern("HH:mm:ss.SSSxxxxx");
    }
}
