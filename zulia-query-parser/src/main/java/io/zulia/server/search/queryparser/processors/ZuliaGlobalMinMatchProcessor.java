package io.zulia.server.search.queryparser.processors;

import org.apache.lucene.queryparser.flexible.core.config.QueryConfigHandler;
import org.apache.lucene.queryparser.flexible.core.nodes.GroupQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessor;
import org.apache.lucene.queryparser.flexible.standard.nodes.MinShouldMatchNode;

import static io.zulia.server.search.queryparser.processors.ZuliaQueryNodeProcessorPipeline.GLOBAL_MM;

public class ZuliaGlobalMinMatchProcessor implements QueryNodeProcessor {

	private QueryConfigHandler queryConfigHandler;

	@Override
	public QueryNode process(QueryNode queryTree) {
		Integer globalMinMatch = getQueryConfigHandler().get(GLOBAL_MM);
		if (globalMinMatch != null && globalMinMatch > 1) {
			return new MinShouldMatchNode(globalMinMatch, new GroupQueryNode(queryTree));
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
