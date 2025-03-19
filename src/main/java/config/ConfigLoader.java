package config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class ConfigLoader {
    private static Config config;

    public static Config loadConfig(String path) throws IOException {
        if (config == null) {
            ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
            config = mapper.readValue(new File(path), Config.class);
        }
        return config;
    }

    public static void initializeDatabase() {
        try (Connection conn = DriverManager.getConnection(config.dbUrl);
             Statement stmt = conn.createStatement()) {
            if (conn != null) {
                String sql = "CREATE TABLE IF NOT EXISTS tradingData (" +
                        "TransactionID INTEGER PRIMARY KEY," +
                        "Date TEXT," +
                        "Time TEXT," +
                        "StockSymbol TEXT," +
                        "Action TEXT," +
                        "Quantity INTEGER," +
                        "Price REAL," +
                        "Name TEXT," +
                        "Kerboros TEXT NOT NULL," +
                        "Comments TEXT" +
                        ");";
                stmt.execute(sql);
            }
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
    }
}
