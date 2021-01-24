package com.github.onozaty.sql.resultset;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;

import org.junit.Test;

/**
 * {@link CsvDestination}のテストです。
 * @author onozaty
 *
 */
public class CsvDestinationTest {

    @Test
    public void test() throws IOException {

        Column<String> column1 = new Column<>("col1", null); // ValueRetrieverは参照しないのでnullで
        Column<Integer> column2 = new Column<>("col2", null);
        Column<Object> column3 = new Column<>("col3", null);

        try (
                StringWriter writer = new StringWriter();
                CsvDestination csvDestination = new CsvDestination(writer)) {

            csvDestination.prepare(Arrays.asList(column1, column2, column3));

            csvDestination.output(column1, "a");
            csvDestination.output(column2, 1);
            csvDestination.output(column3, null);
            csvDestination.endRecord();

            csvDestination.output(column1, "\"");
            csvDestination.output(column2, 1000);
            csvDestination.output(column3, false);
            csvDestination.endRecord();

            String result = writer.toString();
            assertThat(result)
                    .isEqualTo(
                            "col1,col2,col3\r\n"
                                    + "a,1,\r\n"
                                    + "\"\"\"\",1000,false\r\n");
        }
    }

}
