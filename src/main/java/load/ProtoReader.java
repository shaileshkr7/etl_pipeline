package load;

import proto.TradingDataOuterClass.TradingData;
import com.google.protobuf.InvalidProtocolBufferException;

import java.io.FileInputStream;
import java.io.IOException;

public class ProtoReader {
    public void readProtoFile(String inputPath) throws IOException {
        try (FileInputStream input = new FileInputStream(inputPath)) {
            TradingData tradingData;
            while (true) {
                try {
                    tradingData = TradingData.parseDelimitedFrom(input);
                    if (tradingData == null) {
                        break;
                    }
                    printTradingData(tradingData);
                } catch (InvalidProtocolBufferException e) {
                    System.err.println("Failed to parse protobuf message: " + e.getMessage());
                    break;
                }
            }
        }
    }

    private void printTradingData(TradingData tradingData) {
        System.out.println("Transaction ID: " + tradingData.getTransactionId());
        System.out.println("Date: " + tradingData.getTransTime().getDate());
        System.out.println("Time: " + tradingData.getTransTime().getTime());
        System.out.println("Stock Symbol: " + tradingData.getStockSymbol());
        System.out.println("Action: " + tradingData.getAction());
        System.out.println("Quantity: " + tradingData.getQuantity());
        System.out.println("Price: " + tradingData.getPrice());
        System.out.println("Name: " + tradingData.getName());
        System.out.println("Kerberos: " + tradingData.getKerberos());
        System.out.println("Comments: " + tradingData.getComments());
        System.out.println("-----");
    }
}