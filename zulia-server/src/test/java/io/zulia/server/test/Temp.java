package io.zulia.server.test;

import io.zulia.client.command.builder.Search;
import io.zulia.client.command.builder.StatFacet;
import io.zulia.client.config.ZuliaPoolConfig;
import io.zulia.client.pool.ZuliaWorkPool;

public class Temp {

	public static void main(String[] args) throws Exception {

		for (int i = 0; i < 100; i++) {
			ZuliaWorkPool zuliaWorkPool = new ZuliaWorkPool(new ZuliaPoolConfig().addNode("localhost"));

			Search search = new Search("publicgrantsRead");
			//search.addQuery(new ScoredQuery("breast cancers").addQueryFields("title", "abstract"));
			search.addStat(new StatFacet("fy", "k30ClusterId"));
			search.addStat(new StatFacet("fy", "k150ClusterId"));
			search.setDontCache(true);
			System.out.println(zuliaWorkPool.search(search).getCommandTimeMs());
		}
	}
}
