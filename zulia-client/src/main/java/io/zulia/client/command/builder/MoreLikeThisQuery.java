package io.zulia.client.command.builder;

import io.zulia.message.ZuliaQuery;

import java.util.Collection;
import java.util.List;

public class MoreLikeThisQuery implements QueryBuilder {

	private final ZuliaQuery.Query.Builder queryBuilder;
	private final ZuliaQuery.MoreLikeThisParams.Builder mltBuilder;

	/**
	 * Create an MLT query specifying text fields for lexical similarity.
	 * Add source documents via addDocumentId() and/or addLikeText().
	 */
	public MoreLikeThisQuery(String... fields) {
		this(List.of(fields));
	}

	/**
	 * Create an MLT query specifying text fields for lexical similarity.
	 */
	public MoreLikeThisQuery(Collection<String> fields) {
		this.queryBuilder = ZuliaQuery.Query.newBuilder().setQueryType(ZuliaQuery.Query.QueryType.MORE_LIKE_THIS);
		this.mltBuilder = ZuliaQuery.MoreLikeThisParams.newBuilder().addAllField(fields);
	}

	public MoreLikeThisQuery addDocumentId(String docId) {
		mltBuilder.addDocumentId(docId);
		return this;
	}

	public MoreLikeThisQuery addLikeText(String text) {
		mltBuilder.addLikeText(text);
		return this;
	}

	public MoreLikeThisQuery addLikeVector(float[] vector) {
		ZuliaQuery.VectorInput.Builder vi = ZuliaQuery.VectorInput.newBuilder();
		for (float v : vector) {
			vi.addValues(v);
		}
		mltBuilder.addLikeVector(vi);
		return this;
	}

	public MoreLikeThisQuery addLikeVector(double[] vector) {
		ZuliaQuery.VectorInput.Builder vi = ZuliaQuery.VectorInput.newBuilder();
		for (double v : vector) {
			vi.addValues((float) v);
		}
		mltBuilder.addLikeVector(vi);
		return this;
	}

	/**
	 * Add a source vector from any numeric collection (e.g. List&lt;Float&gt; or List&lt;Double&gt;). Values are stored as floats.
	 */
	public MoreLikeThisQuery addLikeVector(Collection<? extends Number> vector) {
		ZuliaQuery.VectorInput.Builder vi = ZuliaQuery.VectorInput.newBuilder();
		for (Number v : vector) {
			vi.addValues(v.floatValue());
		}
		mltBuilder.addLikeVector(vi);
		return this;
	}

	public MoreLikeThisQuery setVectorField(String field) {
		mltBuilder.setVectorField(field);
		return this;
	}

	/**
	 * Add an index to fetch documentId source docs from, in priority order (first index with the doc wins on collision).
	 * When no source index is set, source docs are fetched from the queried indexes.
	 */
	public MoreLikeThisQuery addSourceIndex(String index) {
		mltBuilder.addSourceIndex(index);
		return this;
	}

	public MoreLikeThisQuery setVectorTopN(int topN) {
		mltBuilder.setVectorTopN(topN);
		return this;
	}

	public static MoreLikeThisQuery forDocuments(Collection<String> fields, String... docIds) {
		MoreLikeThisQuery q = new MoreLikeThisQuery(fields);
		for (String docId : docIds) {
			q.addDocumentId(docId);
		}
		return q;
	}

	public static MoreLikeThisQuery forText(Collection<String> fields, String... texts) {
		MoreLikeThisQuery q = new MoreLikeThisQuery(fields);
		for (String text : texts) {
			q.addLikeText(text);
		}
		return q;
	}

	/**
	 * Create a pure-vector MLT query against the given vector field with an explicit KNN vectorTopN.
	 */
	public static MoreLikeThisQuery forVector(String vectorField, float[] vector, int vectorTopN) {
		return new MoreLikeThisQuery().setVectorField(vectorField).addLikeVector(vector).setVectorTopN(vectorTopN);
	}

	/**
	 * Create a pure-vector MLT query against the given vector field with an explicit KNN vectorTopN
	 */
	public static MoreLikeThisQuery forVector(String vectorField, double[] vector, int vectorTopN) {
		return new MoreLikeThisQuery().setVectorField(vectorField).addLikeVector(vector).setVectorTopN(vectorTopN);
	}

	/**
	 * Create a pure-vector MLT query against the given vector field from any numeric collection with an explicit vectorTopN
	 */
	public static MoreLikeThisQuery forVector(String vectorField, Collection<? extends Number> vector, int vectorTopN) {
		return new MoreLikeThisQuery().setVectorField(vectorField).addLikeVector(vector).setVectorTopN(vectorTopN);
	}

	public MoreLikeThisQuery setMinTermFreq(int minTermFreq) {
		mltBuilder.setMinTermFreq(minTermFreq);
		return this;
	}

	public MoreLikeThisQuery setMaxQueryTerms(int maxQueryTerms) {
		mltBuilder.setMaxQueryTerms(maxQueryTerms);
		return this;
	}

	public MoreLikeThisQuery setMinDocFreq(int minDocFreq) {
		mltBuilder.setMinDocFreq(minDocFreq);
		return this;
	}

	public MoreLikeThisQuery setMaxDocFreq(int maxDocFreq) {
		mltBuilder.setMaxDocFreq(maxDocFreq);
		return this;
	}

	/**
	 * Ignore terms that appear in more than {@code maxDocFreqPct} percent (1-100) of the documents in a shard, guarding
	 * against common boilerplate dominating term selection. 0 leaves the default of 25; 100 disables the guard. Ignored
	 * when an absolute {@link #setMaxDocFreq(int)} is set.
	 */
	public MoreLikeThisQuery setMaxDocFreqPct(int maxDocFreqPct) {
		mltBuilder.setMaxDocFreqPct(maxDocFreqPct);
		return this;
	}

	public MoreLikeThisQuery setMinWordLen(int minWordLen) {
		mltBuilder.setMinWordLen(minWordLen);
		return this;
	}

	public MoreLikeThisQuery setMaxWordLen(int maxWordLen) {
		mltBuilder.setMaxWordLen(maxWordLen);
		return this;
	}

	public MoreLikeThisQuery setMaxNumTokensParsed(int maxNumTokensParsed) {
		mltBuilder.setMaxNumTokensParsed(maxNumTokensParsed);
		return this;
	}

	public MoreLikeThisQuery setTextWeight(float textWeight) {
		mltBuilder.setTextWeight(textWeight);
		return this;
	}

	public MoreLikeThisQuery setVectorWeight(float vectorWeight) {
		mltBuilder.setVectorWeight(vectorWeight);
		return this;
	}

	public MoreLikeThisQuery setMinShouldMatch(int minShouldMatch) {
		queryBuilder.setMm(minShouldMatch);
		return this;
	}

	public MoreLikeThisQuery setIncludeSourceDocs(boolean includeSourceDocs) {
		mltBuilder.setIncludeSourceDocs(includeSourceDocs);
		return this;
	}

	@Override
	public ZuliaQuery.Query getQuery() {
		return queryBuilder.setMoreLikeThisParams(mltBuilder).build();
	}
}
