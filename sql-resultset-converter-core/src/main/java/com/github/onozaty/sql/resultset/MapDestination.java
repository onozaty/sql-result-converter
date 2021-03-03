package com.github.onozaty.sql.resultset;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Map形式での出力先です。
 * @author onozaty
 */
public class MapDestination implements OutputDestination {

    private List<Column<?>> columns;
    private Map<String, Object> currentMap;
    private List<Map<String, Object>> records = new ArrayList<Map<String, Object>>();

    @Override
    public void prepare(List<Column<?>> columns) throws IOException {
        this.columns = columns;
    }

    @Override
    public void output(Column<?> column, Object value) throws IOException {

        if (currentMap == null) {
            currentMap = new HashMap<>();
        }

        currentMap.put(column.getName(), value);
    }

    @Override
    public void endRecord() throws IOException {
        records.add(currentMap);
        currentMap = null;
    }

    @Override
    public void close() {
        // 何もしない
    }

    public List<Map<String, Object>> getRecords() {
        return records;
    }

    public List<Column<?>> getColumns() {
        return columns;
    }
}
