package io.zulia.server.index;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.search.ScoreDoc;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.function.IntFunction;

public abstract class ScoredDocLeafHandler<T> {

	public record IndexedScoreDoc(int index, ScoreDoc scoreDoc) {
		int getDocId() {
			return scoreDoc.doc;
		}
	}

	public ScoredDocLeafHandler() {

	}

	public T[] handle(IndexReader indexReader, ScoreDoc[] scoreDocs, IntFunction<T[]> resultArrayConstructor) throws IOException {

		if (scoreDocs.length > 0) {

			IndexedScoreDoc[] zuliaResults = new IndexedScoreDoc[scoreDocs.length];
			for (int i = 0; i < scoreDocs.length; i++) {
				zuliaResults[i] = new IndexedScoreDoc(i, scoreDocs[i]);
			}
			Arrays.sort(zuliaResults, Comparator.comparing(IndexedScoreDoc::getDocId));
			T[] results = resultArrayConstructor.apply(zuliaResults.length);

			int docId = zuliaResults[0].getDocId();

			List<LeafReaderContext> leaves = indexReader.leaves();

			LeafReaderContext currentLeaf = leaves.get(ReaderUtil.subIndex(docId, leaves));
			handleNewLeaf(currentLeaf);
			int endOfCurrentLeaf = currentLeaf.docBase + currentLeaf.reader().maxDoc();
			for (IndexedScoreDoc indexedScoreDoc : zuliaResults) {
				docId = indexedScoreDoc.getDocId();

				if (docId >= endOfCurrentLeaf) {
					currentLeaf = leaves.get(ReaderUtil.subIndex(docId, leaves));
					endOfCurrentLeaf = currentLeaf.docBase + currentLeaf.reader().maxDoc();
					handleNewLeaf(currentLeaf);
				}
				results[indexedScoreDoc.index] = handleDocument(currentLeaf, docId, currentLeaf.docBase, indexedScoreDoc.scoreDoc);
			}

			Arrays.sort(zuliaResults, Comparator.comparing(IndexedScoreDoc::index));
			return results;
		}
		return resultArrayConstructor.apply(0);
	}

	protected abstract void handleNewLeaf(LeafReaderContext currentLeaf) throws IOException;

	protected abstract T handleDocument(LeafReaderContext currentLeaf, int docId, int docBase, ScoreDoc scoreDoc) throws IOException;

}
