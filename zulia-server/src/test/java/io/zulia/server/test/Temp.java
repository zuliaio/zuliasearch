package io.zulia.server.test;

import io.zulia.client.command.builder.CountFacet;
import io.zulia.client.command.builder.Search;
import io.zulia.client.command.builder.Sort;
import io.zulia.client.command.builder.StatFacet;
import io.zulia.client.config.ZuliaPoolConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.SearchResult;
import io.zulia.message.ZuliaQuery;

public class Temp {

	public static void main(String[] args) throws Exception {

		for (int i = 0; i < 10; i++) {
			ZuliaWorkPool zuliaWorkPool = new ZuliaWorkPool(new ZuliaPoolConfig().addNode("localhost", 32193).setNodeUpdateEnabled(false));

			Search search = new Search("publicgrantsRead");

			search.addCountFacet(new CountFacet("fy").setTopN(250));
			search.addCountFacet(new CountFacet("activityCode").setTopN(250));
			search.addCountFacet(new CountFacet("applTypeCode").setTopN(250));
			search.addCountFacet(new CountFacet("adminIcLong").setTopN(250));
			search.addCountFacet(new CountFacet("agency").setTopN(250));

			search.addStat(new StatFacet("r01Project", "k30ClusterId").setTopN(30));
			search.addStat(new StatFacet("r01Project", "k150ClusterId").setTopN(150));
			search.addStat(new StatFacet("r01Project", "k500ClusterId").setTopN(500));
			search.addStat(new StatFacet("awarded", "allPIs.opaId").setTopN(100));
			search.addSort(new Sort("fy").descending());
			search.setDontCache(true);

			SearchResult searchResult = zuliaWorkPool.search(search);
			System.out.println(searchResult.getCommandTimeMs());

			//System.out.println(searchResult.getTotalHits());

			for (ZuliaQuery.FacetGroup facetGroup : searchResult.getFacetGroups()) {
				for (ZuliaQuery.FacetCount facetCount : facetGroup.getFacetCountList()) {
					//System.out.println(facetGroup.getCountRequest().getFacetField().getLabel() + "," + facetCount.getFacet() + "," + facetCount.getCount());
				}
			}

			for (ZuliaQuery.StatGroup statGroup : searchResult.getStatGroups()) {
				for (ZuliaQuery.FacetStats facetStats : statGroup.getFacetStatsList()) {
					//System.out.println(statGroup.getStatRequest().getFacetField().getLabel() + "," + facetStats.getFacet() + "," + facetStats.getSum().getLongValue());
				}
			}

		}
	}
}
