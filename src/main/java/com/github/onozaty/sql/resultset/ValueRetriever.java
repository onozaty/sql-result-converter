package com.github.onozaty.sql.resultset;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

/**
 * {@link ResultSet}から値を取得する処理のインタフェースです。
 * @author onozaty
 */
@FunctionalInterface
public interface ValueRetriever {

    static final DateTimeFormatter DEFAULT_DATE_TIME_FORMATTER = DateTimeFormatter
            .ofPattern("uuuu-MM-dd'T'HH:mm:ss.SSS");
    static final DateTimeFormatter DEFAULT_DATE_FORMATTER = DateTimeFormatter.ofPattern("uuuu-MM-dd");
    static final DateTimeFormatter DEFAULT_TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    /**
     * {@link ResultSet}から値を取得します。
     * @param resultSet 対象の{@link ResultSet}
     * @return 値
     * @throws SQLException
     */
    Object retrieve(ResultSet resultSet) throws SQLException;

    /**
     * 値取得処理を生成します。
     * @param columnIndex カラム位置
     * @return 値取得処理
     */
    public static ValueRetriever newValueRetriever(int columnIndex) {

        return resultSet -> resultSet.getObject(columnIndex);
    }

    /**
     * {@link Timestamp}用の値取得処理を生成します。
     * @param columnIndex カラム位置
     * @return {@link Timestamp}用の値取得処理
     */
    public static ValueRetriever newTimestampValueRetriever(int columnIndex) {

        return newTimestampValueRetriever(columnIndex, DEFAULT_DATE_TIME_FORMATTER);
    }

    /**
     * {@link Timestamp}用の値取得処理を生成します。
     * @param columnIndex カラム位置
     * @param formatter 日時フォーマット
     * @return {@link Timestamp}用の値取得処理
     */
    public static ValueRetriever newTimestampValueRetriever(int columnIndex, DateTimeFormatter formatter) {

        return resultSet -> {

            Timestamp timestamp = resultSet.getTimestamp(columnIndex);

            if (timestamp == null) {
                return null;
            }

            LocalDateTime localDateTime = timestamp.toLocalDateTime();
            return formatter.format(localDateTime);
        };
    }

    /**
     * {@link Date}用の値取得処理を生成します。
     * @param columnIndex カラム位置
     * @return {@link Date}用の値取得処理
     */
    public static ValueRetriever newDateValueRetriever(int columnIndex) {

        return newDateValueRetriever(columnIndex, DEFAULT_DATE_FORMATTER);
    }

    /**
     * {@link Date}用の値取得処理を生成します。
     * @param columnIndex カラム位置
     * @param formatter 日時フォーマット
     * @return {@link Date}用の値取得処理
     */
    public static ValueRetriever newDateValueRetriever(int columnIndex, DateTimeFormatter formatter) {

        return resultSet -> {

            Date date = resultSet.getDate(columnIndex);

            if (date == null) {
                return null;
            }

            LocalDate localDate = date.toLocalDate();
            return formatter.format(localDate);
        };
    }

    /**
     * {@link Time}用の値取得処理を生成します。
     * @param columnIndex カラム位置
     * @return {@link Time}用の値取得処理
     */
    public static ValueRetriever newTimeValueRetriever(int columnIndex) {

        return newTimestampValueRetriever(columnIndex, DEFAULT_TIME_FORMATTER);
    }

    /**
     * {@link Time}用の値取得処理を生成します。
     * @param columnIndex カラム位置
     * @param formatter 日時フォーマット
     * @return {@link Time}用の値取得処理
     */
    public static ValueRetriever newTimeValueRetriever(int columnIndex, DateTimeFormatter formatter) {

        return resultSet -> {

            Time time = resultSet.getTime(columnIndex);

            if (time == null) {
                return null;
            }

            LocalTime localTime = time.toLocalTime();
            return formatter.format(localTime);
        };
    }
}