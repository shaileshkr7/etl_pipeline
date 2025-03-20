package main;

import config.Config;
import config.ConfigLoader;
import extract.CsvExtractor;
import extract.DbExtractor;
import load.ProtoConverter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import transform.Deduplicator;

public class Main {
    public static void main(String[] args) {
        try {
            Config config = ConfigLoader.loadConfig("src/main/resources/config.yaml");
            SparkSession spark = SparkSession.builder()
                    .appName("ETL Pipeline")
                    .master("local[*]") // Set master URL to local
                    .getOrCreate();

            // Extract data from CSV
            CsvExtractor csvExtractor = new CsvExtractor();
            Dataset<Row> csvData = csvExtractor.extract(config.csvFilePath, spark);

            // Extract data from DB
            DbExtractor dbExtractor = new DbExtractor();
            Dataset<Row> dbData = dbExtractor.extract(config.dbUrl, spark);

            // Combine datasets
            Dataset<Row> combinedData = csvData.union(dbData);

            // Deduplicate data
            Deduplicator deduplicator = new Deduplicator();
            Dataset<Row> deduplicatedData = deduplicator.deduplicate(combinedData);

            // Convert to Proto
            ProtoConverter protoConverter = new ProtoConverter();
            protoConverter.convertToProto(deduplicatedData, config.outputPath);

            spark.stop();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}