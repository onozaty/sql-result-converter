package com.github.onozaty.sql.resultset;

import lombok.Value;

/**
 * カラム定義です。
 * @author onozaty
 */
@Value
public class Column {

    /**
     * 名前
     */
    private final String name;

    /**
     * 値取得処理
     */
    private final ValueRetriever retriever;
}
