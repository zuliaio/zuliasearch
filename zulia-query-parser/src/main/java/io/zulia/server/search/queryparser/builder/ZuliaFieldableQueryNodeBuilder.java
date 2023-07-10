package io.zulia.server.search.queryparser.builder;

import io.zulia.server.search.queryparser.node.ZuliaFieldableQueryNode;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.standard.builders.StandardQueryBuilder;
import org.apache.lucene.search.Query;

public class ZuliaFieldableQueryNodeBuilder implements StandardQueryBuilder {

	/**
	 * Constructs a {@link ZuliaFieldableQueryNodeBuilder} object.
	 */
	public ZuliaFieldableQueryNodeBuilder() {
		// empty constructor
	}

	@Override
	public Query build(QueryNode queryNode) throws QueryNodeException {
		ZuliaFieldableQueryNode fieldableQueryNode = (ZuliaFieldableQueryNode) queryNode;
		return fieldableQueryNode.getQuery();
	}
}
