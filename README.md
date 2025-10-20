// src/TemporalAuditDB.java
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * TemporalAuditDB
 * - Stores current items in `items.json` and append-only history in `history.log` (JSON lines)
 * - Provides upsert, get current, travel-to-ts, and diffs
 *
 * Run:
 *   javac src/TemporalAuditDB.java
 *   java -cp src TemporalAuditDB
 */
public class TemporalAuditDB {
    private static final Path ITEMS = Paths.get("items.json");
    private static final Path HISTORY = Paths.get("history.log");

    // in-memory items (loaded from disk if present)
    private final Map<String, String> items = new HashMap<>();

    public TemporalAuditDB() {
        loadItems();
    }

    private void loadItems() {
        try {
            if (Files.exists(ITEMS)) {
                String s = new String(Files.readAllBytes(ITEMS), StandardCharsets.UTF_8).trim();
                if (s.length() > 0) {
                    // simple JSON object mapping id->json-string (we store raw JSON values)
                    // For simplicity we parse as lines of "id\tjson" if items.json not a JSON object.
                    // We'll attempt naive parsing: expect one-per-line id\tjson
                    for (String line : s.split("\n")) {
                        if (line.contains("\t")) {
                            int t = line.indexOf('\t');
                            String id = line.substring(0, t);
                            String json = line.substring(t + 1);
                            items.put(id, json);
                        }
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("Failed to load items: " + e.getMessage());
        }
    }

    private void persistItems() {
        try (BufferedWriter w = Files.newBufferedWriter(ITEMS, StandardCharsets.UTF_8)) {
            for (Map.Entry<String, String> e : items.entrySet()) {
                w.write(e.getKey() + "\t" + e.getValue());
                w.write("\n");
            }
        } catch (IOException ex) {
            System.err.println("Persist error: " + ex.getMessage());
        }
    }

    public void upsert(String id, String jsonData) {
        long ts = Instant.now().getEpochSecond();
        items.put(id, jsonData);
        persistItems();
        String record = ts + "\t" + id + "\t" + jsonData;
        try (BufferedWriter w = Files.newBufferedWriter(HISTORY, StandardOpenOption.CREATE, StandardOpenOption.APPEND)) {
            w.write(record);
            w.write("\n");
        } catch (IOException ex) {
            System.err.println("History write failed: " + ex.getMessage());
        }
        System.out.println("Upserted " + id + " @ " + ts);
    }

    public String get(String id) {
        return items.get(id);
    }

    public String travel(String id, long ts) {
        // read history backward find last entry <= ts
        try {
            if (!Files.exists(HISTORY)) return null;
            List<String> lines = Files.readAllLines(HISTORY, StandardCharsets.UTF_8);
            Collections.reverse(lines);
            for (String line : lines) {
                String[] parts = line.split("\t", 3);
                if (parts.length < 3) continue;
                long recordTs = Long.parseLong(parts[0]);
                String rid = parts[1];
                String j = parts[2];
                if (rid.equals(id) && recordTs <= ts) return j;
            }
        } catch (IOException e) { /* ignore */ }
        return null;
    }

    public List<Map.Entry<Long, String>> diffs(String id) {
        List<Map.Entry<Long, String>> out = new ArrayList<>();
        try {
            if (!Files.exists(HISTORY)) return out;
            for (String line : Files.readAllLines(HISTORY, StandardCharsets.UTF_8)) {
                String[] parts = line.split("\t", 3);
                if (parts.length < 3) continue;
                long recordTs = Long.parseLong(parts[0]);
                String rid = parts[1];
                String j = parts[2];
                if (rid.equals(id)) out.add(new AbstractMap.SimpleEntry<>(recordTs, j));
            }
        } catch (IOException e) { /* ignore */ }
        return out;
    }

    // Demo
    public static void main(String[] args) throws Exception {
        TemporalAuditDB db = new TemporalAuditDB();
        db.upsert("u1", "{\"name\":\"Alice\",\"role\":\"engineer\"}");
        Thread.sleep(1000);
        db.upsert("u1", "{\"name\":\"Alice\",\"role\":\"senior engineer\"}");
        Thread.sleep(1000);
        db.upsert("u1", "{\"name\":\"Alice\",\"role\":\"team lead\"}");
        System.out.println("Current: " + db.get("u1"));
        List<Map.Entry<Long, String>> history = db.diffs("u1");
        System.out.println("History entries:");
        for (Map.Entry<Long, String> e : history) {
            System.out.println(" - " + e.getKey() + " => " + e.getValue());
        }
        if (history.size() >= 2) {
            long mid = history.get(1).getKey();
            System.out.println("Travel to " + mid + " -> " + db.travel("u1", mid));
        }
    }
}
