package io.zulia.server.search.queryparser.zulia.builders;

import io.zulia.server.search.queryparser.zulia.nodes.MinMatchQueryNode;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.builders.QueryTreeBuilder;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.standard.builders.StandardQueryBuilder;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;

/**
 * This builder basically reads the {@link Query} object set on the {@link MinMatchQueryNode} child
 * using {@link QueryTreeBuilder#QUERY_TREE_BUILDER_TAGID} and applies the minimum should match
 * value defined in the {@link MinMatchQueryNode}.
 */
public class MinMatchQueryNodeBuilder implements StandardQueryBuilder {

	public MinMatchQueryNodeBuilder() {
		// empty constructor
	}

	@Override
	public Query build(QueryNode queryNode) throws QueryNodeException {
		MinMatchQueryNode minMatchQueryNode = (MinMatchQueryNode) queryNode;
		QueryNode child = minMatchQueryNode.getChild();

		if (child == null) {
			return null;
		}

		Object tag = child.getTag(QueryTreeBuilder.QUERY_TREE_BUILDER_TAGID);
		if (tag instanceof BooleanQuery) {

			BooleanQuery query = (BooleanQuery) tag;

			BooleanQuery.Builder builder = new BooleanQuery.Builder();

			for (BooleanClause clause : query.clauses()) {
				builder.add(clause);
			}
			builder.setMinimumNumberShouldMatch(minMatchQueryNode.getValue());

			return builder.build();
		}
		else {
			throw new QueryNodeException(new IllegalArgumentException("Minimum should match can only be set on a group of terms"));
		}
	}
}
