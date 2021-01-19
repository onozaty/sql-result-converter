package com.github.onozaty.sql.resultset;

import lombok.Value;

/**
 * カラム定義です。
 * @author onozaty
 * @param <T> カラムの値型
 */
@Value
public class Column<T> {

    /**
     * 名前
     */
    private final String name;

    /**
     * 値取得処理
     */
    private final ValueRetriever<T> retriever;
}
