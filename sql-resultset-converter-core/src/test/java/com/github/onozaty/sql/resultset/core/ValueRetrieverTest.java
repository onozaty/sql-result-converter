package com.github.onozaty.sql.resultset.core;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.sql.Array;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

import org.junit.Test;

/**
 * {@link ValueRetriever}のテストです。
 * @author onozaty
 */
public class ValueRetrieverTest {

    /**
     * {@link ValueRetriever#newValueRetriever(int, Class)}のテストです。
     * <p>INT型のカラムの確認です。</p>
     * @throws SQLException
     */
    @Test
    public void newValueRetriever_Int() throws SQLException {

        try (Connection connection = TestDbUtils.getConnection()) {

            TestDbUtils.setupTable(connection);

            try (Statement statement = connection.createStatement()) {

                try (ResultSet resultSet = statement.executeQuery("SELECT column_int FROM rows")) {

                    resultSet.next();

                    Integer value = ValueRetriever.newValueRetriever(1, Integer.class).retrieve(resultSet);

                    assertThat(value)
                            .isEqualTo(Integer.valueOf(1));
                }
            }
        }
    }

    /**
     * {@link ValueRetriever#newValueRetriever(int, Class)}のテストです。
     * <p>BOOLEAN型のカラムの確認です。</p>
     * @throws SQLException
     */
    @Test
    public void newValueRetriever_Boolean() throws SQLException {

        try (Connection connection = TestDbUtils.getConnection()) {

            TestDbUtils.setupTable(connection);

            try (Statement statement = connection.createStatement()) {

                try (ResultSet resultSet = statement.executeQuery("SELECT column_boolean FROM rows")) {

                    resultSet.next();

                    Boolean value = ValueRetriever.newValueRetriever(1, Boolean.class).retrieve(resultSet);

                    assertThat(value)
                            .isEqualTo(Boolean.valueOf(true));
                }
            }
        }
    }

    /**
     * {@link ValueRetriever#newValueRetriever(int, Class)}のテストです。
     * <p>TINYINT型のカラムの確認です。</p>
     * @throws SQLException
     */
    @Test
    public void newValueRetriever_Tinyint() throws SQLException {

        try (Connection connection = TestDbUtils.getConnection()) {

            TestDbUtils.setupTable(connection);

            try (Statement statement = connection.createStatement()) {

                try (ResultSet resultSet = statement.executeQuery("SELECT column_tinyint FROM rows")) {

                    resultSet.next();

                    Byte value = ValueRetriever.newValueRetriever(1, Byte.class).retrieve(resultSet);

                    assertThat(value)
                            .isEqualTo(Byte.valueOf((byte) -1));
                }
            }
        }
    }

    /**
     * {@link ValueRetriever#newValueRetriever(int, Class)}のテストです。
     * <p>SMALLINT型のカラムの確認です。</p>
     * @throws SQLException
     */
    @Test
    public void newValueRetriever_Samllint() throws SQLException {

        try (Connection connection = TestDbUtils.getConnection()) {

            TestDbUtils.setupTable(connection);

            try (Statement statement = connection.createStatement()) {

                try (ResultSet resultSet = statement.executeQuery("SELECT column_smallint FROM rows")) {

                    resultSet.next();

                    Short value = ValueRetriever.newValueRetriever(1, Short.class).retrieve(resultSet);

                    assertThat(value)
                            .isEqualTo(Short.valueOf((short) 100));
                }
            }
        }
    }

    /**
     * {@link ValueRetriever#newValueRetriever(int, Class)}のテストです。
     * <p>BIGINT型のカラムの確認です。</p>
     * @throws SQLException
     */
    @Test
    public void newValueRetriever_Bigint() throws SQLException {

        try (Connection connection = TestDbUtils.getConnection()) {

            TestDbUtils.setupTable(connection);

            try (Statement statement = connection.createStatement()) {

                try (ResultSet resultSet = statement.executeQuery("SELECT column_bigint FROM rows")) {

                    resultSet.next();

                    Long value = ValueRetriever.newValueRetriever(1, Long.class).retrieve(resultSet);

                    assertThat(value)
                            .isEqualTo(Long.valueOf(200000));
                }
            }
        }
    }

    /**
     * {@link ValueRetriever#newValueRetriever(int, Class)}のテストです。
     * <p>DECIMAL型のカラムの確認です。</p>
     * @throws SQLException
     */
    @Test
    public void newValueRetriever_Decimal() throws SQLException {

        try (Connection connection = TestDbUtils.getConnection()) {

            TestDbUtils.setupTable(connection);

            try (Statement statement = connection.createStatement()) {

                try (ResultSet resultSet = statement.executeQuery("SELECT column_decimal FROM rows")) {

                    resultSet.next();

                    BigDecimal value = ValueRetriever.newValueRetriever(1, BigDecimal.class).retrieve(resultSet);

                    assertThat(value)
                            .isEqualTo(BigDecimal.valueOf(1234.123));
                }
            }
        }
    }

    /**
     * {@link ValueRetriever#newValueRetriever(int, Class)}のテストです。
     * <p>DOUBLE型のカラムの確認です。</p>
     * @throws SQLException
     */
    @Test
    public void newValueRetriever_Double() throws SQLException {

        try (Connection connection = TestDbUtils.getConnection()) {

            TestDbUtils.setupTable(connection);

            try (Statement statement = connection.createStatement()) {

                try (ResultSet resultSet = statement.executeQuery("SELECT column_double FROM rows")) {

                    resultSet.next();

                    Double value = ValueRetriever.newValueRetriever(1, Double.class).retrieve(resultSet);

                    assertThat(value)
                            .isEqualTo(Double.valueOf(1.1));
                }
            }
        }
    }

    /**
     * {@link ValueRetriever#newValueRetriever(int, Class)}のテストです。
     * <p>REAL型のカラムの確認です。</p>
     * @throws SQLException
     */
    @Test
    public void newValueRetriever_Real() throws SQLException {

        try (Connection connection = TestDbUtils.getConnection()) {

            TestDbUtils.setupTable(connection);

            try (Statement statement = connection.createStatement()) {

                try (ResultSet resultSet = statement.executeQuery("SELECT column_real FROM rows")) {

                    resultSet.next();

                    Float value = ValueRetriever.newValueRetriever(1, Float.class).retrieve(resultSet);

                    assertThat(value)
                            .isEqualTo(Float.valueOf((float) 2.2));
                }
            }
        }
    }

    /**
     * {@link ValueRetriever#newValueRetriever(int, Class)}のテストです。
     * <p>TIME型のカラムの確認です。</p>
     * @throws SQLException
     */
    @Test
    public void newValueRetriever_Time() throws SQLException {

        try (Connection connection = TestDbUtils.getConnection()) {

            TestDbUtils.setupTable(connection);

            try (Statement statement = connection.createStatement()) {

                try (ResultSet resultSet = statement.executeQuery("SELECT column_time FROM rows")) {

                    resultSet.next();

                    Time value = ValueRetriever.newValueRetriever(1, Time.class).retrieve(resultSet);

                    assertThat(value)
                            .isEqualTo(Time.valueOf(LocalTime.of(23, 59, 59)));
                }
            }
        }
    }

    /**
     * {@link ValueRetriever#newValueRetriever(int, Class)}のテストです。
     * <p>TIME WITH TIME ZONE型のカラムの確認です。</p>
     * @throws SQLException
     */
    @Test
    public void newValueRetriever_TimeWithTz() throws SQLException {

        try (Connection connection = TestDbUtils.getConnection()) {

            TestDbUtils.setupTable(connection);

            try (Statement statement = connection.createStatement()) {

                try (ResultSet resultSet = statement.executeQuery("SELECT column_time_tz FROM rows")) {

                    resultSet.next();

                    OffsetTime value = ValueRetriever.newValueRetriever(1, OffsetTime.class).retrieve(resultSet);

                    assertThat(value)
                            .isEqualTo(OffsetTime.of(23, 59, 59, 0, ZoneOffset.ofHours(1)));
                }
            }
        }
    }

    /**
     * {@link ValueRetriever#newValueRetriever(int, Class)}のテストです。
     * <p>DATE型のカラムの確認です。</p>
     * @throws SQLException
     */
    @Test
    public void newValueRetriever_Date() throws SQLException {

        try (Connection connection = TestDbUtils.getConnection()) {

            TestDbUtils.setupTable(connection);

            try (Statement statement = connection.createStatement()) {

                try (ResultSet resultSet = statement.executeQuery("SELECT column_date FROM rows")) {

                    resultSet.next();

                    Date value = ValueRetriever.newValueRetriever(1, Date.class).retrieve(resultSet);

                    assertThat(value)
                            .isEqualTo(Date.valueOf(LocalDate.of(2004, 12, 31)));
                }
            }
        }
    }

    /**
     * {@link ValueRetriever#newValueRetriever(int, Class)}のテストです。
     * <p>TIMESTAMP型のカラムの確認です。</p>
     * @throws SQLException
     */
    @Test
    public void newValueRetriever_Timestamp() throws SQLException {

        try (Connection connection = TestDbUtils.getConnection()) {

            TestDbUtils.setupTable(connection);

            try (Statement statement = connection.createStatement()) {

                try (ResultSet resultSet = statement.executeQuery("SELECT column_timestamp FROM rows")) {

                    resultSet.next();

                    Timestamp value = ValueRetriever.newValueRetriever(1, Timestamp.class).retrieve(resultSet);

                    assertThat(value)
                            .isEqualTo(Timestamp.valueOf(LocalDateTime.of(1999, 1, 31, 10, 0, 0)));
                }
            }
        }
    }

    /**
     * {@link ValueRetriever#newValueRetriever(int, Class)}のテストです。
     * <p>TIMESTAMP WITH TIME ZONE型のカラムの確認です。</p>
     * @throws SQLException
     */
    @Test
    public void newValueRetriever_TimestampWithTz() throws SQLException {

        try (Connection connection = TestDbUtils.getConnection()) {

            TestDbUtils.setupTable(connection);

            try (Statement statement = connection.createStatement()) {

                try (ResultSet resultSet = statement.executeQuery("SELECT column_timestamp_tz FROM rows")) {

                    resultSet.next();

                    OffsetDateTime value = ValueRetriever.newValueRetriever(1, OffsetDateTime.class)
                            .retrieve(resultSet);

                    assertThat(value)
                            .isEqualTo(OffsetDateTime.of(2005, 12, 31, 23, 59, 59, 0, ZoneOffset.ofHours(-10)));
                }
            }
        }
    }

    /**
     * {@link ValueRetriever#newValueRetriever(int, Class)}のテストです。
     * <p>VARCHAR型のカラムの確認です。</p>
     * @throws SQLException
     */
    @Test
    public void newValueRetriever_Varchar() throws SQLException {

        try (Connection connection = TestDbUtils.getConnection()) {

            TestDbUtils.setupTable(connection);

            try (Statement statement = connection.createStatement()) {

                try (ResultSet resultSet = statement.executeQuery("SELECT column_varchar FROM rows")) {

                    resultSet.next();

                    String value = ValueRetriever.newValueRetriever(1, String.class).retrieve(resultSet);

                    assertThat(value)
                            .isEqualTo("xxxxxxx");
                }
            }
        }
    }

    /**
     * {@link ValueRetriever#newValueRetriever(int, Class)}のテストです。
     * <p>ARRAY型のカラムの確認です。</p>
     * @throws SQLException
     */
    @Test
    public void newValueRetriever_Array() throws SQLException {

        try (Connection connection = TestDbUtils.getConnection()) {

            TestDbUtils.setupTable(connection);

            try (Statement statement = connection.createStatement()) {

                try (ResultSet resultSet = statement.executeQuery("SELECT column_array FROM rows")) {

                    resultSet.next();

                    Array value = ValueRetriever.newValueRetriever(1, Array.class).retrieve(resultSet);

                    assertThat((Object[]) value.getArray())
                            .containsExactly(1, 2);
                }
            }
        }
    }

    /**
     * {@link ValueRetriever#newFormattedValueRetriever(int, Class, DateTimeFormatter)}のテストです。
     * <p>TIME型のカラムの確認です。</p>
     * @throws SQLException
     */
    @Test
    public void newFormattedValueRetriever_Time() throws SQLException {

        try (Connection connection = TestDbUtils.getConnection()) {

            TestDbUtils.setupTable(connection);

            try (Statement statement = connection.createStatement()) {

                try (ResultSet resultSet = statement.executeQuery("SELECT column_time FROM rows")) {

                    resultSet.next();

                    String value = ValueRetriever
                            .newFormattedValueRetriever(1, LocalTime.class, DateTimeFormatter.ISO_LOCAL_TIME)
                            .retrieve(resultSet);

                    assertThat(value)
                            .isEqualTo("23:59:59");
                }
            }
        }
    }

    /**
     * {@link ValueRetriever#newFormattedValueRetriever(int, Class, DateTimeFormatter)}のテストです。
     * <p>TIME WITH TIME ZONE型のカラムの確認です。</p>
     * @throws SQLException
     */
    @Test
    public void newFormattedValueRetriever_TimeWithTz() throws SQLException {

        try (Connection connection = TestDbUtils.getConnection()) {

            TestDbUtils.setupTable(connection);

            try (Statement statement = connection.createStatement()) {

                try (ResultSet resultSet = statement.executeQuery("SELECT column_time_tz FROM rows")) {

                    resultSet.next();

                    String value = ValueRetriever
                            .newFormattedValueRetriever(1, OffsetTime.class, DateTimeFormatter.ISO_OFFSET_TIME)
                            .retrieve(resultSet);

                    assertThat(value)
                            .isEqualTo("23:59:59+01:00");
                }
            }
        }
    }

    /**
     * {@link ValueRetriever#newFormattedValueRetriever(int, Class, DateTimeFormatter)}のテストです。
     * <p>DATE型のカラムの確認です。</p>
     * @throws SQLException
     */
    @Test
    public void newFormattedValueRetriever_Date() throws SQLException {

        try (Connection connection = TestDbUtils.getConnection()) {

            TestDbUtils.setupTable(connection);

            try (Statement statement = connection.createStatement()) {

                try (ResultSet resultSet = statement.executeQuery("SELECT column_date FROM rows")) {

                    resultSet.next();

                    String value = ValueRetriever
                            .newFormattedValueRetriever(1, LocalDate.class, DateTimeFormatter.ISO_LOCAL_DATE)
                            .retrieve(resultSet);

                    assertThat(value)
                            .isEqualTo("2004-12-31");
                }
            }
        }
    }

    /**
     * {@link ValueRetriever#newFormattedValueRetriever(int, Class, DateTimeFormatter)}のテストです。
     * <p>TIMESTAMP型のカラムの確認です。</p>
     * @throws SQLException
     */
    @Test
    public void newFormattedValueRetriever_Timestamp() throws SQLException {

        try (Connection connection = TestDbUtils.getConnection()) {

            TestDbUtils.setupTable(connection);

            try (Statement statement = connection.createStatement()) {

                try (ResultSet resultSet = statement.executeQuery("SELECT column_timestamp FROM rows")) {

                    resultSet.next();

                    String value = ValueRetriever
                            .newFormattedValueRetriever(1, LocalDateTime.class, DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                            .retrieve(resultSet);

                    assertThat(value)
                            .isEqualTo("1999-01-31T10:00:00");
                }
            }
        }
    }

    /**
     * {@link ValueRetriever#newFormattedValueRetriever(int, Class, DateTimeFormatter)}のテストです。
     * <p>TIMESTAMP WITH TIME ZONE型のカラムの確認です。</p>
     * @throws SQLException
     */
    @Test
    public void newFormattedValueRetriever_TimestampWithTz() throws SQLException {

        try (Connection connection = TestDbUtils.getConnection()) {

            TestDbUtils.setupTable(connection);

            try (Statement statement = connection.createStatement()) {

                try (ResultSet resultSet = statement.executeQuery("SELECT column_timestamp_tz FROM rows")) {

                    resultSet.next();

                    String value = ValueRetriever
                            .newFormattedValueRetriever(1, OffsetDateTime.class, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                            .retrieve(resultSet);

                    assertThat(value)
                            .isEqualTo("2005-12-31T23:59:59-10:00");
                }
            }
        }
    }

}
