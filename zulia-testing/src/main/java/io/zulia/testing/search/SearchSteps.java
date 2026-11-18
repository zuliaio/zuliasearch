package io.zulia.testing.search;

import io.zulia.client.command.builder.Search;
import io.zulia.client.result.SearchResult;
import io.zulia.testing.config.SearchConfig;
import io.zulia.testing.js.dto.QueryResultObject;

/**
 * The two steps of a search that a caller may replace: building the request and shaping the result.
 */
public interface SearchSteps {

	Search buildSearch(String indexName, SearchConfig searchConfig);

	QueryResultObject buildQueryResultObject(SearchResult searchResult, SearchConfig searchConfig);
}
