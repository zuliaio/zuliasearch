package io.zulia.server.test;

import io.zulia.client.command.builder.ScoredQuery;
import io.zulia.client.command.builder.Search;
import io.zulia.client.command.builder.Sort;
import io.zulia.client.config.ZuliaPoolConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.SearchResult;
import io.zulia.message.ZuliaQuery;

public class QueryTest {

    public static void main(String[] args) throws Exception {
        ZuliaWorkPool zuliaWorkPool = new ZuliaWorkPool(new ZuliaPoolConfig().addNode("localhost"));

        Search search = new Search("publicgrants20220303");
        search.setAmount(500);
        search.addQuery(new ScoredQuery("breast AND cancer").addQueryFields("title", "abstract"));
        search.addDocumentFields("title", "abstract");

        SearchResult searchResult = zuliaWorkPool.search(search);
        System.out.println(searchResult.getTotalHits());
        System.out.println("breast AND cancer Top 100," + searchResult.getCommandTimeMs());

        search.addSort(new Sort("title"));
        searchResult = zuliaWorkPool.search(search);
        System.out.println("breast AND cancer Top 100 Sort Title," + searchResult.getCommandTimeMs());

        search.clearSort();
        long start = System.currentTimeMillis();
        zuliaWorkPool.searchAllAsDocument(search, document -> {
            //no-op
        });
        long end = System.currentTimeMillis();
        System.out.println("breast AND cancer All Matches," + (end - start));

        search.setResultFetchType(ZuliaQuery.FetchType.NONE);
        search.clearSort();
        search.clearLastResult();
        search.addSort(new Sort("id"));
        start = System.currentTimeMillis();
        zuliaWorkPool.searchAllAsScoredResult(search, scoredResult -> {
            //no-op
        });
        end = System.currentTimeMillis();
        System.out.println("breast AND cancer All Matches Sort Id (None)," + (end - start));

        search.setResultFetchType(ZuliaQuery.FetchType.FULL);
        search.clearSort();
        search.clearLastResult();
        search.addSort(new Sort("id"));
        start = System.currentTimeMillis();
        zuliaWorkPool.searchAllAsDocument(search, document -> {
            //no-op
        });
        end = System.currentTimeMillis();
        System.out.println("breast AND cancer All Matches Sort Id (Full)," + (end - start));

    }
}
