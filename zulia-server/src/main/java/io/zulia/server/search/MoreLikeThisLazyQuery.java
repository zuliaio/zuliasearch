package io.zulia.server.search;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queries.mlt.MoreLikeThis;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;

import java.io.IOException;
import java.io.StringReader;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * A lazy {@link Query} wrapper that defers MoreLikeThis query construction until
 * {@link #rewrite(IndexSearcher)} time, when an {@link IndexSearcher} (and its
 * {@link org.apache.lucene.index.IndexReader} for IDF stats) is available.
 * <p>
 * MLT needs IDF statistics from the IndexReader, but query construction in
 * {@code ZuliaIndex.getQuery()} happens before shard dispatch. Each shard rewrites
 * this into a real {@link org.apache.lucene.search.BooleanQuery} using its own reader.
 * <p>
 * Per-shard IDF means term scoring varies across shards on a multi-shard index;
 * this is the standard MLT trade-off in distributed search.
 */
public final class MoreLikeThisLazyQuery extends Query {

	private final List<String> likeTexts;
	private final List<String> fields;
	private final Analyzer analyzer;
	private final int minTermFreq;
	private final int maxQueryTerms;
	private final int minDocFreq;
	private final int maxDocFreq;
	private final int minWordLen;
	private final int maxWordLen;
	private final int maxNumTokensParsed;
	private final int minShouldMatch;

	public MoreLikeThisLazyQuery(Collection<String> likeTexts, Collection<String> fields, Analyzer analyzer, int minTermFreq, int maxQueryTerms, int minDocFreq,
			int maxDocFreq, int minWordLen, int maxWordLen, int maxNumTokensParsed, int minShouldMatch) {
		this.likeTexts = List.copyOf(likeTexts);
		this.fields = List.copyOf(fields);
		this.analyzer = analyzer;
		this.minTermFreq = minTermFreq;
		this.maxQueryTerms = maxQueryTerms;
		this.minDocFreq = minDocFreq;
		this.maxDocFreq = maxDocFreq;
		this.minWordLen = minWordLen;
		this.maxWordLen = maxWordLen;
		this.maxNumTokensParsed = maxNumTokensParsed;
		this.minShouldMatch = minShouldMatch;
	}

	@Override
	public Query rewrite(IndexSearcher indexSearcher) throws IOException {
		MoreLikeThis mlt = new MoreLikeThis(indexSearcher.getIndexReader());
		mlt.setAnalyzer(analyzer);
		mlt.setFieldNames(fields.toArray(String[]::new));
		mlt.setMinTermFreq(minTermFreq);
		mlt.setMaxQueryTerms(maxQueryTerms);
		mlt.setMinDocFreq(minDocFreq);
		if (maxDocFreq > 0) {
			mlt.setMaxDocFreq(maxDocFreq);
		}
		mlt.setMinWordLen(minWordLen);
		if (maxWordLen > 0) {
			mlt.setMaxWordLen(maxWordLen);
		}
		mlt.setMaxNumTokensParsed(maxNumTokensParsed);
		mlt.setBoost(true);

		// Use the reader overload per-field; the Map<String, Collection<Object>> overload returned empty queries
		// when given Reader values in Lucene 10.x. For multi-field, OR the per-field results together.
		// minShouldMatch applies to the term disjunction of each field separately.
		if (fields.size() == 1) {
			return applyMinShouldMatch(mlt.like(fields.getFirst(), buildReaders()));
		}

		BooleanQuery.Builder combined = new BooleanQuery.Builder();
		for (String field : fields) {
			combined.add(applyMinShouldMatch(mlt.like(field, buildReaders())), BooleanClause.Occur.SHOULD);
		}
		return combined.build();
	}

	private Query applyMinShouldMatch(Query query) {
		if (minShouldMatch <= 0 || !(query instanceof BooleanQuery booleanQuery)) {
			return query;
		}
		BooleanQuery.Builder builder = new BooleanQuery.Builder().setMinimumNumberShouldMatch(minShouldMatch);
		for (BooleanClause clause : booleanQuery) {
			builder.add(clause);
		}
		return builder.build();
	}

	private StringReader[] buildReaders() {
		StringReader[] readers = new StringReader[likeTexts.size()];
		for (int i = 0; i < likeTexts.size(); i++) {
			readers[i] = new StringReader(likeTexts.get(i));
		}
		return readers;
	}

	@Override
	public void visit(QueryVisitor visitor) {
		visitor.visitLeaf(this);
	}

	@Override
	public String toString(String field) {
		return "MoreLikeThisLazyQuery{fields=" + fields + ", texts=" + likeTexts.size() + ", minTermFreq=" + minTermFreq + ", maxQueryTerms=" + maxQueryTerms
				+ ", minDocFreq=" + minDocFreq + ", maxDocFreq=" + maxDocFreq + ", minWordLen=" + minWordLen + ", maxWordLen=" + maxWordLen
				+ ", maxNumTokensParsed=" + maxNumTokensParsed + ", minShouldMatch=" + minShouldMatch + "}";
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (!(o instanceof MoreLikeThisLazyQuery other))
			return false;
		return minTermFreq == other.minTermFreq && maxQueryTerms == other.maxQueryTerms && minDocFreq == other.minDocFreq && maxDocFreq == other.maxDocFreq
				&& minWordLen == other.minWordLen && maxWordLen == other.maxWordLen && maxNumTokensParsed == other.maxNumTokensParsed
				&& minShouldMatch == other.minShouldMatch && analyzer == other.analyzer && likeTexts.equals(other.likeTexts) && fields.equals(other.fields);
	}

	@Override
	public int hashCode() {
		return Objects.hash(likeTexts, fields, System.identityHashCode(analyzer), minTermFreq, maxQueryTerms, minDocFreq, maxDocFreq, minWordLen, maxWordLen,
				maxNumTokensParsed, minShouldMatch);
	}
}
