package fluss.integration;

import java.time.Duration;
import java.util.Map;
import org.apache.fluss.client.ConnectionFactory;
import org.apache.fluss.config.Configuration;
import org.apache.fluss.metadata.TablePath;
import org.apache.fluss.record.ChangeType;

class ReadLogRow {
    public static void main(String[] args) throws Exception {
        var config = Configuration.fromMap(Map.of("bootstrap.servers", args[0]));
        try (var connection = ConnectionFactory.createConnection(config);
                var table = connection.getTable(TablePath.of(args[1], args[2]));
                var scanner = table.newScan().createLogScanner()) {
            scanner.subscribeFromBeginning(0);
            for (int i = 0; i < 10; i++) {
                var rows = scanner.poll(Duration.ofSeconds(1)).iterator();
                if (!rows.hasNext()) {
                    continue;
                }
                var entry = rows.next();
                var row = entry.getRow();
                if (entry.getChangeType() != ChangeType.APPEND_ONLY
                        || row.getInt(0) != 1 || !args[3].equals(row.getString(1).toString())) {
                    throw new AssertionError("read unexpected row: " + row);
                }
                return;
            }
            throw new AssertionError("no row returned by the Java scanner");
        }
    }
}
