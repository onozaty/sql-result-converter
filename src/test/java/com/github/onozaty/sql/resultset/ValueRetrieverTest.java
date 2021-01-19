package com.github.onozaty.sql.resultset;

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

import org.junit.Test;

/**
 * {@link ValueRetriever}のテストです。
 * @author onozaty
 */
public class ValueRetrieverTest {

    /**
     * INT型のカラムに対するテストです。
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
     * BOOLEAN型のカラムに対するテストです。
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
     * TINYINT型のカラムに対するテストです。
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
     * SMALLINT型のカラムに対するテストです。
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
     * BIGINT型のカラムに対するテストです。
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
     * DECIMAL型のカラムに対するテストです。
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
     * DOUBLE型のカラムに対するテストです。
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
     * REAL型のカラムに対するテストです。
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
     * TIME型のカラムに対するテストです。
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
     * TIME WITH TIME ZONE型のカラムに対するテストです。
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
     * DATE型のカラムに対するテストです。
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
     * TIMESTAMP型のカラムに対するテストです。
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
     * TIMESTAMP WITH TIME ZONE型のカラムに対するテストです。
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
     * VARCHAR型のカラムに対するテストです。
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
     * ARRAY型のカラムに対するテストです。
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
}
