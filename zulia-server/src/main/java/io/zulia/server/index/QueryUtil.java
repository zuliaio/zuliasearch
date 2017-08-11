package io.zulia.server.index;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;

import java.util.Collection;

public class QueryUtil {

	/** From org.apache.solr.search.QueryUtils **/
	public static boolean isNegative(Query q) {
		if (!(q instanceof BooleanQuery))
			return false;
		BooleanQuery bq = (BooleanQuery) q;
		Collection<BooleanClause> clauses = bq.clauses();
		if (clauses.size() == 0)
			return false;
		for (BooleanClause clause : clauses) {
			if (!clause.isProhibited())
				return false;
		}
		return true;
	}

	/** Fixes a negative query by adding a MatchAllDocs query clause.
	 * The query passed in *must* be a negative query.
	 */
	public static Query fixNegativeQuery(Query q) {
		float boost = 1f;
		if (q instanceof BoostQuery) {
			BoostQuery bq = (BoostQuery) q;
			boost = bq.getBoost();
			q = bq.getQuery();
		}
		BooleanQuery bq = (BooleanQuery) q;
		BooleanQuery.Builder newBqB = new BooleanQuery.Builder();
		newBqB.setDisableCoord(bq.isCoordDisabled());
		newBqB.setMinimumNumberShouldMatch(bq.getMinimumNumberShouldMatch());
		for (BooleanClause clause : bq) {
			newBqB.add(clause);
		}
		newBqB.add(new MatchAllDocsQuery(), BooleanClause.Occur.MUST);
		BooleanQuery newBq = newBqB.build();
		return new BoostQuery(newBq, boost);
	}
}
