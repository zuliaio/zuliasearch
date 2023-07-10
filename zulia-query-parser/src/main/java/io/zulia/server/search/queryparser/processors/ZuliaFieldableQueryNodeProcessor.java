package io.zulia.server.search.queryparser.processors;

import io.zulia.server.config.IndexFieldInfo;
import io.zulia.server.config.ServerIndexConfig;
import io.zulia.server.search.queryparser.node.ZuliaFieldableQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessorImpl;

import java.util.List;

public class ZuliaFieldableQueryNodeProcessor extends QueryNodeProcessorImpl {

	/**
	 * Constructs a {@link ZuliaFieldableQueryNodeProcessor} object.
	 */
	public ZuliaFieldableQueryNodeProcessor() {
		// empty constructor
	}

	@Override
	protected QueryNode postProcessNode(QueryNode node) {

		if (node instanceof ZuliaFieldableQueryNode zuliaFieldableQueryNode) {
			String field = zuliaFieldableQueryNode.getField().toString();
			ServerIndexConfig indexConfig = getQueryConfigHandler().get(ZuliaQueryNodeProcessorPipeline.ZULIA_INDEX_CONFIG);
			IndexFieldInfo indexFieldInfo = indexConfig.getIndexFieldInfo(field);
			if (indexFieldInfo != null) {
				zuliaFieldableQueryNode.setIndexFieldInfo(indexFieldInfo);
			}
		}
		return node;
	}

	@Override
	protected QueryNode preProcessNode(QueryNode node) {
		return node;
	}

	@Override
	protected List<QueryNode> setChildrenOrder(List<QueryNode> children) {
		return children;
	}
}
