package io.zulia.server.search.queryparser.builders;

import io.zulia.server.search.queryparser.nodes.MinMatchQueryNode;
import org.apache.lucene.queryparser.flexible.standard.builders.StandardQueryTreeBuilder;

public class ZuliaStandardQueryTreeBuilder extends StandardQueryTreeBuilder {

	public ZuliaStandardQueryTreeBuilder() {
		super();
		setBuilder(MinMatchQueryNode.class, new MinMatchQueryNodeBuilder());
	}

}
