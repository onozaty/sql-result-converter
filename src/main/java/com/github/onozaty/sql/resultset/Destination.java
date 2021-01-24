package com.github.onozaty.sql.resultset;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * 出力先を表します。
 * @author onozaty
 */
public interface Destination extends Closeable {

    /**
     * 準備します。
     * @param columns カラム一覧
     * @throws IOException
     */
    void prepare(List<Column<?>> columns) throws IOException;

    /**
     * 値を出力します。
     * @param <T> 出力する値の型
     * @param column カラム
     * @param value 値
     * @throws IOException
     */
    <T> void output(Column<T> column, T value) throws IOException;

    /**
     * 行を終了します。
     * @throws IOException
     */
    void endRecord() throws IOException;
}
