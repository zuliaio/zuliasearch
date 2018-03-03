package io.zulia.server.analysis.frequency;

import io.zulia.server.index.ReaderAndState;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.TFIDFSimilarity;

import java.io.IOException;
import java.util.HashMap;

/**
 * Created by Matt Davis on 6/28/16.
 * @author mdavis
 */
public class DocFreq {

	private final HashMap<String, Integer> docFreqMap;
	private final ReaderAndState readerAndState;
	private final String field;
	private final TFIDFSimilarity similarity;
	private final int numDocs;

	public DocFreq(ReaderAndState readerAndState, String field) {
		this.readerAndState = readerAndState;
		this.field = field;
		this.docFreqMap = new HashMap<>();
		this.similarity = new ClassicSimilarity();
		this.numDocs = readerAndState.numDocs();
	}

	public int getDocFreq(String term) throws IOException {
		Integer termDocFreq = this.docFreqMap.get(term);
		if (termDocFreq == null) {
			termDocFreq = readerAndState.docFreq(field, term);
			docFreqMap.put(term, termDocFreq);
		}

		return termDocFreq;

	}

	public double getScoreForTerm(long termFreq, long docFreq) {
		return similarity.tf(termFreq) * similarity.idf(docFreq, numDocs);
	}

	public int getNumDocsForPercent(float percent) {
		return Math.round(numDocs * percent);
	}

}
