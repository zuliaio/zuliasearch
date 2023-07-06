package io.zulia.server.search.queryparser.builder;

import io.zulia.server.search.queryparser.node.ZuliaFieldableQueryNode;
import io.zulia.server.search.queryparser.node.ZuliaPointRangeQueryNode;
import org.apache.lucene.queryparser.flexible.standard.builders.StandardQueryTreeBuilder;

public class ZuliaQueryTreeBuilder extends StandardQueryTreeBuilder {

	public ZuliaQueryTreeBuilder() {
		setBuilder(ZuliaPointRangeQueryNode.class, new ZuliaPointRangeQueryNodeBuilder());
		setBuilder(ZuliaFieldableQueryNode.class, new ZuliaFieldableQueryNodeBuilder());
	}

}
