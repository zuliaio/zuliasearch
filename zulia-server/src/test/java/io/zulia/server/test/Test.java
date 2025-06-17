package io.zulia.server.test;

import io.zulia.client.command.builder.Search;
import io.zulia.client.command.builder.StatFacet;
import io.zulia.client.config.ZuliaPoolConfig;
import io.zulia.client.pool.ZuliaPool;
import io.zulia.client.pool.ZuliaWorkPool;

public class Test {
	public static void main(String[] args) throws Exception {
		try (ZuliaWorkPool zuliaWorkPool = new ZuliaWorkPool(new ZuliaPoolConfig().addNode("localhost"))) {

			Search search = new Search("grantsRead");
			search.addStat(new StatFacet("research","k150ClusterId"));
			zuliaWorkPool.search(search);
		}
	}
}
