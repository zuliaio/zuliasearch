package io.zulia.util.schema;

import org.bson.Document;

import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;

public class DocumentStats {

    private final TreeMap<String, FieldStats> keyToFieldStat;

    public DocumentStats() {
        keyToFieldStat = new TreeMap<>();
    }

    public void tallyDocument(Document document) {
        tallyDocument(document, "");
    }

    private void tallyDocument(Object o, String prefix) {

        if (o instanceof Document document) {
            keyToFieldStat.computeIfAbsent(prefix, FieldStats::new).tallyDocument();
            for (Map.Entry<String, Object> entry : document.entrySet()) {
                String newPrefix = prefix.isEmpty() ? entry.getKey() : prefix + "." + entry.getKey();
                tallyDocument(entry.getValue(), newPrefix);
            }

        } else if (o instanceof Collection collection) {
            String newPrefix = prefix + "[]";
            for (Object v : collection) {
                tallyDocument(v, newPrefix);
            }
            keyToFieldStat.computeIfAbsent(newPrefix, FieldStats::new).tallyCollection();
        } else {
            keyToFieldStat.computeIfAbsent(prefix, FieldStats::new).tallyType(o);
        }
    }

    public TreeMap<String, FieldStats> getKeyToFieldStat() {
        return keyToFieldStat;
    }

    public void addDocStats(String key, FieldStats fieldStats) {
        FieldStats tallyStat = keyToFieldStat.computeIfAbsent(key, FieldStats::new);
        tallyStat.incrementByIfExists(fieldStats);
    }

    public void addTermStats(String key, FieldStats fieldStats) {
        FieldStats tallyStat = keyToFieldStat.computeIfAbsent(key, FieldStats::new);
        tallyStat.incrementByCounts(fieldStats);

    }
}