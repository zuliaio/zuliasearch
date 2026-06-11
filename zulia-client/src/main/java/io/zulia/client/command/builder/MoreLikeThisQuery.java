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

	public MoreLikeThisQuery setVectorField(String field) {
		mltBuilder.setVectorField(field);
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
