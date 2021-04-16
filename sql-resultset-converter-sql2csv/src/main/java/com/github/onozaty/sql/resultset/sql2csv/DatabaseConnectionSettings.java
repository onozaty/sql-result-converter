package com.github.onozaty.sql.resultset.sql2csv;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * DB接続情報です。
 * @author onozaty
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class DatabaseConnectionSettings {

    private static final ObjectMapper objectMapper = new ObjectMapper();

    private String url;

    private String user;

    private String password;

    /**
     * JSONファイルから読み込みます。
     * @param jsonPath JSONファイルパス
     * @return インスタンス
     * @throws JsonParseException
     * @throws JsonMappingException
     * @throws IOException
     */
    public static DatabaseConnectionSettings of(Path jsonPath)
            throws JsonParseException, JsonMappingException, IOException {
        return objectMapper.readValue(jsonPath.toFile(), DatabaseConnectionSettings.class);
    }

    /**
     * DBコネクションを取得します。
     * @return DBコネクション
     * @throws SQLException
     */
    public Connection getConnection() throws SQLException {

        return DriverManager.getConnection(url, user, password);
    }

}
