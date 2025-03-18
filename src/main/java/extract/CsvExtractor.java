package extract;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class CsvExtractor {
    public Dataset<Row> extract(String filePath, SparkSession spark) {
        return spark.read().format("csv").option("header", "true").load(filePath);
    }
}
