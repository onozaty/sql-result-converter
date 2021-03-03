package com.github.onozaty.sql.resultset;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;

/**
 * {@link ResultSet}から値を取得する処理のインタフェースです。
 * @author onozaty
 * @param <T> 値の型
 */
@FunctionalInterface
public interface ValueRetriever<T> {

    /**
     * {@link ResultSet}から値を取得します。
     * @param resultSet 対象の{@link ResultSet}
     * @return 値
     * @throws SQLException
     */
    T retrieve(ResultSet resultSet) throws SQLException;

    /**
     * 値取得処理を生成します。
     * @param <T> 値の型
     * @param columnIndex カラム位置
     * @param type 取得する値の型
     * @return 値取得処理
     */
    public static <T> ValueRetriever<T> newValueRetriever(int columnIndex, Class<T> type) {

        return resultSet -> resultSet.getObject(columnIndex, type);
    }

    /**
     * 値取得処理を生成します。
     * @param <T> {@link ResultSet}から取得した値の型
     * @param <R> 変換処理後の型
     * @param columnIndex カラム位置
     * @param type 取得する値の型
     * @param converter 値の変換処理
     * @return 値取得処理
     */
    public static <T, R> ValueRetriever<R> newValueRetriever(
            int columnIndex, Class<T> type, ValueConverter<T, R> converter) {

        return resultSet -> {
            T value = resultSet.getObject(columnIndex, type);
            return converter.apply(value);
        };
    }

    /**
     * 日時をフォーマットする値取得処理を生成します。
     * @param columnIndex カラム位置
     * @param type 取得する値の型
     * @param formatter フォーマット
     * @return 値取得処理
     */
    public static ValueRetriever<String> newFormattedValueRetriever(
            int columnIndex, Class<? extends TemporalAccessor> type, DateTimeFormatter formatter) {

        return resultSet -> {

            TemporalAccessor value = resultSet.getObject(columnIndex, type);

            if (value == null) {
                return null;
            }

            return formatter.format(value);
        };
    }

    @FunctionalInterface
    public static interface ValueConverter<T, R> {
        R apply(T t) throws SQLException;
    }
}