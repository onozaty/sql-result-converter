package com.github.onozaty.sql.resultset;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;

import org.junit.Test;

/**
 * {@link CsvResultWriter}のテストです。
 * @author onozaty
 *
 */
public class CsvResultWriterTest {

    @Test
    public void test() throws IOException {

        Column<String> column1 = new Column<>("col1", null); // ValueRetrieverは参照しないのでnullで
        Column<Integer> column2 = new Column<>("col2", null);
        Column<Object> column3 = new Column<>("col3", null);

        try (
                StringWriter writer = new StringWriter();
                CsvResultWriter resultWriter = new CsvResultWriter(writer)) {

            resultWriter.prepare(Arrays.asList(column1, column2, column3));

            resultWriter.write(column1, "a");
            resultWriter.write(column2, 1);
            resultWriter.write(column3, null);
            resultWriter.endRecord();

            resultWriter.write(column1, "\"");
            resultWriter.write(column2, 1000);
            resultWriter.write(column3, false);
            resultWriter.endRecord();

            String result = writer.toString();
            assertThat(result)
                    .isEqualTo(
                            "col1,col2,col3\r\n"
                                    + "a,1,\r\n"
                                    + "\"\"\"\",1000,false\r\n");
        }
    }

}
