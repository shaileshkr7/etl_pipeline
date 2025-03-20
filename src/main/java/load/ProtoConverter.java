package load;

import com.google.protobuf.Message;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.FileOutputStream;
import java.io.IOException;

public class ProtoConverter {
    public void convertToProto(Dataset<Row> dataset, String outputPath) throws IOException {
        // Example conversion logic
        for (Row row : dataset.collectAsList()) {
            // Convert each row to a Proto message
            Message protoMessage = convertRowToProto(row);
            // Write the Proto message to a file
            try (FileOutputStream output = new FileOutputStream(outputPath, true)) {
                protoMessage.writeTo(output);
            }
        }
    }

    private Message convertRowToProto(Row row) {
        // Implement the conversion logic here
        // Example: return TradingDataProto.newBuilder().setField(row.getString(0)).build();
        // Replace with actual implementation
        return TradingDataProto.newBuilder()
                .setTransactionID(row.getInt(0))
                .setDate(row.getString(1))
                .setTime(row.getString(2))
                .setStockSymbol(row.getString(3))
                .setAction(row.getString(4))
                .setQuantity(row.getInt(5))
                .setPrice(row.getDouble(6))
                .setName(row.getString(7))
                .setKerboros(row.getString(8))
                .setComments(row.getString(9))
                .build();
    }
}
