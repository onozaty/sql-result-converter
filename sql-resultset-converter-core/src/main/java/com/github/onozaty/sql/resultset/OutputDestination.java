package com.github.onozaty.sql.resultset;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * 出力先を表します。
 * @author onozaty
 */
public interface OutputDestination extends Closeable {

    /**
     * 準備します。
     * @param columns カラム一覧
     * @throws IOException
     */
    void prepare(List<Column<?>> columns) throws IOException;

    /**
     * 値を出力します。
     * @param column カラム
     * @param value 値
     * @throws IOException
     */
    void output(Column<?> column, Object value) throws IOException;

    /**
     * 行を終了します。
     * @throws IOException
     */
    void endRecord() throws IOException;
}
