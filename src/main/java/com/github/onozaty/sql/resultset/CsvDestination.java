package com.github.onozaty.sql.resultset;

import java.io.IOException;
import java.io.Writer;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

/**
 * CSV形式での出力先です。
 * @author onozaty
 */
public class CsvDestination implements Destination {

    private final CSVPrinter csvPrinter;

    /**
     * コンストラクタ
     * @param writer Writer
     * @throws IOException
     */
    public CsvDestination(Writer writer) throws IOException {
        this(new CSVPrinter(writer, CSVFormat.EXCEL));
    }

    /**
     * コンストラクタ
     * @param csvPrinter CSV出力
     */
    public CsvDestination(CSVPrinter csvPrinter) {
        this.csvPrinter = csvPrinter;
    }

    @Override
    public void prepare(List<Column<?>> columns) throws IOException {

        // ヘッダとして出力
        csvPrinter.printRecord(
                columns.stream()
                        .map(Column::getName)
                        .collect(Collectors.toList()));
    }

    @Override
    public <T> void output(Column<T> column, T value) throws IOException {
        csvPrinter.print(value);
    }

    @Override
    public void endRecord() throws IOException {
        csvPrinter.println();
    }

    @Override
    public void close() throws IOException {
        csvPrinter.close();
    }
}
