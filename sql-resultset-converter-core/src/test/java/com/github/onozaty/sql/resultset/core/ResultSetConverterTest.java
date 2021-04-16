package com.github.onozaty.sql.resultset.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import org.junit.Test;

/**
 * {@link ResultSetConverter}のテストです。
 * @author onozaty
 */
public class ResultSetConverterTest {

    /**
     * {@link ResultSetConverter#convert(java.sql.Connection, String, OutputDestination)}のテストです。
     * @throws SQLException
     * @throws IOException
     */
    @Test
    public void convert() throws SQLException, IOException {

        try (Connection connection = TestDbUtils.getConnection()) {

            TestDbUtils.setupTable(connection);

            MapDestination mapDestination = new MapDestination();
            int count = ResultSetConverter.convert(
                    connection,
                    "SELECT * FROM rows",
                    mapDestination);

            List<Map<String, Object>> expected = new ArrayList<>();
            expected.add(
                    new MapBuilder()
                            .entry("COLUMN_INT", Integer.valueOf(1))
                            .entry("COLUMN_BOOLEAN", Boolean.valueOf(true))
                            .entry("COLUMN_TINYINT", Byte.valueOf((byte) -1))
                            .entry("COLUMN_SMALLINT", Short.valueOf((short) 100))
                            .entry("COLUMN_BIGINT", Long.valueOf(200000))
                            .entry("COLUMN_DECIMAL", new BigDecimal("1234.123"))
                            .entry("COLUMN_DOUBLE", Double.valueOf(1.1))
                            .entry("COLUMN_REAL", Float.valueOf((float) 2.2))
                            .entry("COLUMN_TIME", "23:59:59.000")
                            .entry("COLUMN_TIME_TZ", "23:59:59.000+01:00")
                            .entry("COLUMN_DATE", "2004-12-31")
                            .entry("COLUMN_TIMESTAMP", "1999-01-31 10:00:00.000")
                            .entry("COLUMN_TIMESTAMP_TZ", "2005-12-31 23:59:59.000-10:00")
                            .entry("COLUMN_VARCHAR", "xxxxxxx")
                            .entry("COLUMN_ARRAY", Arrays.asList(1, 2))
                            .build());
            expected.add(
                    new MapBuilder()
                            .entry("COLUMN_INT", Integer.valueOf(2))
                            .entry("COLUMN_BOOLEAN", Boolean.valueOf(false))
                            .entry("COLUMN_TINYINT", Byte.valueOf((byte) 123))
                            .entry("COLUMN_SMALLINT", null)
                            .entry("COLUMN_BIGINT", Long.valueOf(-123456))
                            .entry("COLUMN_DECIMAL", new BigDecimal("1.000"))
                            .entry("COLUMN_DOUBLE", Double.valueOf(0.001))
                            .entry("COLUMN_REAL", Float.valueOf((float) -10.0))
                            .entry("COLUMN_TIME", "00:00:00.123")
                            .entry("COLUMN_TIME_TZ", "12:00:01.222+09:00")
                            .entry("COLUMN_DATE", null)
                            .entry("COLUMN_TIMESTAMP", "2021-12-24 20:00:00.123")
                            .entry("COLUMN_TIMESTAMP_TZ", "2020-02-01 23:59:59.333+00:00")
                            .entry("COLUMN_VARCHAR", "")
                            .entry("COLUMN_ARRAY", Arrays.asList("a"))
                            .build());

            equals(expected.get(0), mapDestination.getRecords().get(0));

            assertThat(mapDestination.getRecords())
                    .containsExactlyElementsOf(expected);
            assertThat(count).isEqualTo(2);
        }

    }

    /**
     * {@link ResultSetConverter#convert(java.sql.Connection, String, OutputDestination)}のテストです。
     * @throws SQLException
     * @throws IOException
     */
    @Test
    public void convert_カラム名() throws SQLException, IOException {

        try (Connection connection = TestDbUtils.getConnection()) {

            TestDbUtils.setupTable(connection);

            MapDestination mapDestination = new MapDestination();
            int count = ResultSetConverter.convert(
                    connection,
                    "SELECT COLUMN_INT AS カラム1, COLUMN_BOOLEAN AS カラム2, COLUMN_TINYINT FROM rows",
                    mapDestination);

            List<Map<String, Object>> expected = new ArrayList<>();
            expected.add(
                    new MapBuilder()
                            .entry("カラム1", Integer.valueOf(1))
                            .entry("カラム2", Boolean.valueOf(true))
                            .entry("COLUMN_TINYINT", Byte.valueOf((byte) -1))
                            .build());
            expected.add(
                    new MapBuilder()
                            .entry("カラム1", Integer.valueOf(2))
                            .entry("カラム2", Boolean.valueOf(false))
                            .entry("COLUMN_TINYINT", Byte.valueOf((byte) 123))
                            .build());

            equals(expected.get(0), mapDestination.getRecords().get(0));

            assertThat(mapDestination.getRecords())
                    .containsExactlyElementsOf(expected);
            assertThat(count).isEqualTo(2);
        }

    }

    private static class MapBuilder {

        private Map<String, Object> map = new HashMap<>();

        public MapBuilder entry(String key, Object value) {

            map.put(key, value);
            return this;
        }

        public Map<String, Object> build() {
            return map;
        }
    }

    private boolean equals(Map<String, Object> map1, Map<String, Object> map2) {

        for (Entry<String, Object> entry : map1.entrySet()) {

            Object other = map2.get(entry.getKey());

            if (!Objects.equals(other, entry.getValue())) {
                return false;
            }

        }

        return true;
    }
}
