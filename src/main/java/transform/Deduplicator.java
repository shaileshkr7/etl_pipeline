package transform;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class Deduplicator {
    public Dataset<Row> deduplicate(Dataset<Row> dataset) {
        return dataset.dropDuplicates();
    }
}
