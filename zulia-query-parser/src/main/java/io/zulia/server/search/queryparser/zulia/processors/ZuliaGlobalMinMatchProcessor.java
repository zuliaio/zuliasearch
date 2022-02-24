package io.zulia.server.search.queryparser.zulia.processors;

import io.zulia.server.search.queryparser.zulia.nodes.MinMatchQueryNode;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.config.QueryConfigHandler;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessor;

import static io.zulia.server.search.queryparser.zulia.processors.ZuliaQueryNodeProcessorPipeline.GLOBAL_MM;

public class ZuliaGlobalMinMatchProcessor implements QueryNodeProcessor {

	private QueryConfigHandler queryConfigHandler;

	@Override
	public QueryNode process(QueryNode queryTree) throws QueryNodeException {
		Integer globalMinMatch = getQueryConfigHandler().get(GLOBAL_MM);
		if (globalMinMatch != null && globalMinMatch > 1) {
			return new MinMatchQueryNode(queryTree, globalMinMatch);
		}
		return queryTree;
	}

	@Override
	public void setQueryConfigHandler(QueryConfigHandler queryConfigHandler) {
		this.queryConfigHandler = queryConfigHandler;
	}

	@Override
	public QueryConfigHandler getQueryConfigHandler() {
		return queryConfigHandler;
	}
}
