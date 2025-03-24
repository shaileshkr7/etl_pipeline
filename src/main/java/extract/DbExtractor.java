package extract;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class DbExtractor {
    public Dataset<Row> extract(String url, SparkSession spark) {
        Properties connectionProperties = new Properties();
        return spark.read().jdbc(url, "tradingData", connectionProperties);
    }
}
