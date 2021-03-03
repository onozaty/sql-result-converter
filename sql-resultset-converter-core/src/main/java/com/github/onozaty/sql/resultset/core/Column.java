package com.github.onozaty.sql.resultset.core;

/**
 * カラム定義です。
 * @author onozaty
 * @param <T> カラムの値型
 */
public class Column<T> {

    /**
     * 名前
     */
    private final String name;

    /**
     * 値取得処理
     */
    private final ValueRetriever<T> retriever;

    /**
     * コンストラクタ
     * @param name 名前
     * @param retriever 値取得処理
     */
    public Column(String name, ValueRetriever<T> retriever) {
        this.name = name;
        this.retriever = retriever;
    }

    public String getName() {
        return name;
    }

    public ValueRetriever<T> getRetriever() {
        return retriever;
    }
}
