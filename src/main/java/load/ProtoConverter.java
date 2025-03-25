package load;

import com.google.protobuf.Message;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import proto.TradingDataOuterClass.TradingData;
import proto.TradingDataOuterClass.TradingData.TransTime;

import java.io.FileOutputStream;
import java.io.IOException;

public class ProtoConverter {
    public void convertToProto(Dataset<Row> dataset, String outputPath) throws IOException {
        // Example conversion logic
        try (FileOutputStream output = new FileOutputStream(outputPath, true)) {
            for (Row row : dataset.collectAsList()) {
                // Convert each row to a Proto message
                Message protoMessage = convertRowToProto(row);
                // Write the Proto message to a file
                protoMessage.writeDelimitedTo(output);
            }
        }
    }

    private Message convertRowToProto(Row row) {
        // Implement the conversion logic here
        TransTime transTime = TransTime.newBuilder()
                .setDate(row.getString(1))
                .setTime(row.getString(2))
                .build();

        return TradingData.newBuilder()
                .setTransactionId(Integer.parseInt(row.getString(0)))
                .setStockSymbol(row.getString(3))
                .setAction(row.getString(4))
                .setQuantity(Integer.parseInt(row.getString(5)))
                .setPrice(Double.parseDouble(row.getString(6)))
                .setName(row.getString(7))
                .setKerberos(row.getString(8))
                .setComments(row.getString(9))
                .setTransTime(transTime)
                .build();
    }
}