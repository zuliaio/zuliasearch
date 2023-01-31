package io.zulia.util.schema;

import org.bson.Document;

import java.util.Map;

public class CollectionStats {
	private final DocumentStats docFreq = new DocumentStats();
	private final DocumentStats termFreq = new DocumentStats();

	private int totalDocs = 0;

	public void tallyDocument(Document document) {
		DocumentStats documentStats = new DocumentStats();
		documentStats.tallyDocument(document);
		totalDocs++;
		documentStats.getKeyToFieldStat().forEach((key, fieldStats) -> {
			docFreq.addDocStats(key, fieldStats);
			termFreq.addTermStats(key, fieldStats);
		});
	}

	public DocumentStats getDocFreq() {
		return docFreq;
	}

	public DocumentStats getTermFreq() {
		return termFreq;
	}

	public int getTotalDocs() {
		return totalDocs;
	}

	public void displayDocFreqStats() {
		for (Map.Entry<String, FieldStats> keyToFieldStats : docFreq.getKeyToFieldStat().entrySet()) {
			FieldStats value = keyToFieldStats.getValue();
			System.out.println(keyToFieldStats.getKey() + " --> " + value.displayDocStats(totalDocs));
		}
	}

	public void displayPerDocFreqStats() {

		for (Map.Entry<String, FieldStats> keyToFieldStats : termFreq.getKeyToFieldStat().entrySet()) {
			FieldStats value = keyToFieldStats.getValue();
			System.out.println(keyToFieldStats.getKey() + " --> " + value.displayFieldPerDocStats(totalDocs));
		}
	}
}