package extract;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class DbExtractor {
    public Dataset<Row> extract(String url, String user, String password, SparkSession spark) {
        Properties connectionProperties = new Properties();
        connectionProperties.put("user", user);
        connectionProperties.put("password", password);
        return spark.read().jdbc(url, "trading_data", connectionProperties);
    }
}
