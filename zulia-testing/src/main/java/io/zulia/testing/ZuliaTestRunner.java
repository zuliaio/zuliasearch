package io.zulia.testing;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import io.zulia.client.command.builder.CountFacet;
import io.zulia.client.command.builder.FilterQuery;
import io.zulia.client.command.builder.NumericStat;
import io.zulia.client.command.builder.Search;
import io.zulia.client.command.builder.StatFacet;
import io.zulia.client.config.ZuliaPoolConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.CompleteResult;
import io.zulia.client.result.SearchResult;
import io.zulia.message.ZuliaQuery;
import io.zulia.testing.config.ConnectionConfig;
import io.zulia.testing.config.FacetConfig;
import io.zulia.testing.config.IndexConfig;
import io.zulia.testing.config.NumStatConfig;
import io.zulia.testing.config.QueryConfig;
import io.zulia.testing.config.StatFacetConfig;
import io.zulia.testing.config.TestConfig;
import io.zulia.testing.config.ZuliaTestConfig;
import io.zulia.testing.js.dto.DocumentProxyObject;
import io.zulia.testing.js.dto.FacetValueObject;
import io.zulia.testing.js.dto.KeyedListProxyObject;
import io.zulia.testing.js.dto.KeyedProxyObject;
import io.zulia.testing.js.dto.PercentileValueObject;
import io.zulia.testing.js.dto.QueryResultObject;
import io.zulia.testing.js.dto.StatFacetValueObject;
import io.zulia.testing.result.TestResult;
import io.zulia.util.exception.ThrowingSupplier;
import org.bson.Document;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.proxy.ProxyObject;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ZuliaTestRunner {
	private static final Logger LOG = LoggerFactory.getLogger(ZuliaTestRunner.class);

	protected final ZuliaTestConfig zuliaTestConfig;
	private final Map<String, Supplier<ZuliaWorkPool>> connectionToConnectionConfig;

	public ZuliaTestRunner(ZuliaTestConfig zuliaTestConfig) {
		this.zuliaTestConfig = zuliaTestConfig;
		this.connectionToConnectionConfig = buildConnectionSupplier();
	}

	public List<TestResult> runTests() throws Exception {
		Map<String, QueryResultObject> resultMap = buildAndRunQueries();
		return evaluateTestsWithQueryResults(resultMap);
	}

	@NotNull
	protected Map<String, Supplier<ZuliaWorkPool>> buildConnectionSupplier() {
		Map<String, ConnectionConfig> connectionIdToConnectionConfig = zuliaTestConfig.getConnections();
		Map<String, IndexConfig> indexNameToIndexConfig = zuliaTestConfig.getIndexes();
		Map<String, Supplier<ZuliaWorkPool>> connectionToConnectionConfig = new HashMap<>();
		for (Map.Entry<String, ConnectionConfig> indexConfigEntry : connectionIdToConnectionConfig.entrySet()) {
			ConnectionConfig connectionConfig = indexConfigEntry.getValue();

			ThrowingSupplier<ZuliaWorkPool> throwingSupplier = () -> {
				ZuliaPoolConfig zuliaPoolConfig = new ZuliaPoolConfig().addNode(connectionConfig.getServerAddress(), connectionConfig.getPort());
				return new ZuliaWorkPool(zuliaPoolConfig);
			};

			connectionToConnectionConfig.put(indexConfigEntry.getKey(), Suppliers.memoize(throwingSupplier));
		}

		for (Map.Entry<String, IndexConfig> indexConfigEntry : indexNameToIndexConfig.entrySet()) {
			IndexConfig indexConfig = indexConfigEntry.getValue();
			Supplier<ZuliaWorkPool> zuliaWorkPoolSupplier = connectionToConnectionConfig.get(indexConfig.getConnection());
			if (zuliaWorkPoolSupplier == null) {
				throw new IllegalArgumentException(
						"Failed to find connection config <" + indexConfig.getConnection() + "> for query <" + indexConfigEntry.getKey() + ">");
			}
		}
		return connectionToConnectionConfig;
	}

	@NotNull
	protected Map<String, QueryResultObject> buildAndRunQueries() throws Exception {
		Map<String, QueryConfig> queries = zuliaTestConfig.getSearches();
		Map<String, IndexConfig> indexNameToIndexConfig = zuliaTestConfig.getIndexes();

		Map<String, QueryResultObject> resultMap = new HashMap<>();
		for (Map.Entry<String, QueryConfig> queryConfigEntry : queries.entrySet()) {
			QueryConfig queryConfig = queryConfigEntry.getValue();
			IndexConfig indexConfig = indexNameToIndexConfig.get(queryConfig.getIndex());
			ZuliaWorkPool zuliaWorkPool = connectionToConnectionConfig.get(indexConfig.getConnection()).get();
			Search s = buildSearch(indexConfig.getIndexName(), queryConfig);
			if (zuliaTestConfig.isLogSearches()) {
				LOG.info("Running search <" + queryConfigEntry.getKey() + ">:\n" + s);
			}
			SearchResult searchResult = zuliaWorkPool.search(s);
			QueryResultObject result = buildQueryResultObject(searchResult, queryConfig);
			resultMap.put(queryConfigEntry.getKey(), result);
		}
		return resultMap;
	}

	@NotNull
	protected Search buildSearch(String indexName, QueryConfig queryConfig) {
		Search s = new Search(indexName);
		s.addQuery(new FilterQuery(queryConfig.getQuery()));
		s.setAmount(queryConfig.getAmount());

		if (queryConfig.getFacets() != null) {
			for (FacetConfig facet : queryConfig.getFacets()) {
				CountFacet countFacet = new CountFacet(facet.getField());
				if (facet.getTopN() != 0) {
					countFacet.setTopN(facet.getTopN());
				}
				s.addCountFacet(countFacet);
			}
		}

		if (queryConfig.getStatFacets() != null) {
			for (StatFacetConfig statFacetConfig : queryConfig.getStatFacets()) {
				StatFacet statFacet = new StatFacet(statFacetConfig.getNumericField(), statFacetConfig.getFacetField());
				if (statFacetConfig.getTopN() != 0) {
					statFacet.setTopN(statFacetConfig.getTopN());
				}
				s.addStat(statFacet);
			}
		}
		if (queryConfig.getNumStats() != null) {
			for (NumStatConfig numStatConfig : queryConfig.getNumStats()) {
				NumericStat numericStat = new NumericStat(numStatConfig.getNumericField());
				if (numStatConfig.getPercentilePrecision() > 0) {
					numericStat.setPercentilePrecision(numStatConfig.getPercentilePrecision());
				}
				if (numStatConfig.getPercentiles() != null) {
					numericStat.setPercentiles(numStatConfig.getPercentiles());
				}
				s.addStat(numericStat);
			}
		}

		return s;
	}

	@NotNull
	protected QueryResultObject buildQueryResultObject(SearchResult searchResult, QueryConfig queryConfig) {
		QueryResultObject result = new QueryResultObject();
		result.count = searchResult.getTotalHits();

		if (queryConfig.getAmount() > 0) {
			List<ProxyObject> docs = new ArrayList<>();
			for (CompleteResult completeResult : searchResult.getCompleteResults()) {
				Document document = completeResult.getDocument();
				docs.add(new DocumentProxyObject(document));
			}
			result.doc = docs;
		}

		if (queryConfig.getFacets() != null) {
			Map<String, List<FacetValueObject>> facetResults = new HashMap<>();
			for (FacetConfig facet : queryConfig.getFacets()) {
				List<FacetValueObject> facetValues = new ArrayList<>();
				List<ZuliaQuery.FacetCount> facetCounts = searchResult.getFacetCounts(facet.getField());
				for (ZuliaQuery.FacetCount facetCount : facetCounts) {
					FacetValueObject fv = new FacetValueObject();
					fv.label = facetCount.getFacet();
					fv.count = facetCount.getCount();
					facetValues.add(fv);
				}
				facetResults.put(facet.getField(), facetValues);
			}
			result.facet = new KeyedListProxyObject<>(facetResults);
		}

		if (queryConfig.getStatFacets() != null) {
			Map<String, List<StatFacetValueObject>> statFacetResults = new HashMap<>();
			for (StatFacetConfig statFacetConfig : queryConfig.getStatFacets()) {
				List<StatFacetValueObject> statFacetValuesObjects = new ArrayList<>();
				List<ZuliaQuery.FacetStats> statFacets = searchResult.getFacetFieldStat(statFacetConfig.getNumericField(), statFacetConfig.getFacetField());
				for (ZuliaQuery.FacetStats statFacet : statFacets) {
					StatFacetValueObject statFacetValueObject = getStatFacetValueObject(statFacet);
					statFacetValuesObjects.add(statFacetValueObject);
				}
				statFacetResults.put(statFacetConfig.getFacetField() + "-" + statFacetConfig.getNumericField(), statFacetValuesObjects);
			}
			result.statFacet = new KeyedListProxyObject<>(statFacetResults);
		}

		if (queryConfig.getNumStats() != null) {
			Map<String, StatFacetValueObject> numStatResults = new HashMap<>();
			for (NumStatConfig numStatConfig : queryConfig.getNumStats()) {
				ZuliaQuery.FacetStats numericFieldStat = searchResult.getNumericFieldStat(numStatConfig.getNumericField());
				StatFacetValueObject statFacetValueObject = getStatFacetValueObject(numericFieldStat);
				numStatResults.put(numStatConfig.getNumericField(), statFacetValueObject);
			}
			result.numStat = new KeyedProxyObject<>(numStatResults);
		}

		return result;
	}

	private static @NotNull StatFacetValueObject getStatFacetValueObject(ZuliaQuery.FacetStats statFacet) {
		StatFacetValueObject statFacetValueObject = new StatFacetValueObject();
		statFacetValueObject.label = statFacet.getFacet();
		statFacetValueObject.docCount = statFacet.getDocCount();
		statFacetValueObject.allDocCount = statFacet.getAllDocCount();
		statFacetValueObject.valueCount = statFacet.getValueCount();
		if (statFacet.getSum().getDoubleValue() > statFacet.getSum().getLongValue()) {
			statFacetValueObject.sum = statFacet.getSum().getDoubleValue();
			statFacetValueObject.max = statFacet.getMax().getDoubleValue();
			statFacetValueObject.min = statFacet.getMin().getDoubleValue();
		}
		else {
			statFacetValueObject.sum = statFacet.getSum().getLongValue();
			statFacetValueObject.max = statFacet.getMax().getLongValue();
			statFacetValueObject.min = statFacet.getMin().getLongValue();
		}
		if (!statFacet.getPercentilesList().isEmpty()) {
			List<PercentileValueObject> percentileValueObjects = new ArrayList<>();
			for (ZuliaQuery.Percentile percentile : statFacet.getPercentilesList()) {
				PercentileValueObject percentileValueObject = new PercentileValueObject();
				percentileValueObject.point = percentile.getPoint();
				percentileValueObject.value = percentile.getValue();
				percentileValueObjects.add(percentileValueObject);
			}
			statFacetValueObject.percentiles = percentileValueObjects;
		}
		return statFacetValueObject;
	}


	@NotNull
	protected List<TestResult> evaluateTestsWithQueryResults(Map<String, QueryResultObject> resultMap) {
		List<TestResult> testResults = new ArrayList<>();
		try (Context context = Context.newBuilder("js").option("engine.WarnInterpreterOnly", "false").allowHostAccess(HostAccess.ALL).build()) {
			Value jsValue = context.getBindings("js");
			for (Map.Entry<String, QueryResultObject> countToEntry : resultMap.entrySet()) {
				jsValue.putMember(countToEntry.getKey(), countToEntry.getValue());
				if (zuliaTestConfig.isLogSearchResults()) {
					String json = context.eval("js", "JSON.stringify(" + countToEntry.getKey() + ")").asString();
					LOG.info("Search result <" + countToEntry.getKey() + ">:\n" + json);
				}
			}

			for (Map.Entry<String, TestConfig> testConfigEntry : zuliaTestConfig.getTests().entrySet()) {
				LOG.info("Running Test <" + testConfigEntry.getKey() + ">");
				TestConfig testConfig = testConfigEntry.getValue();

				TestResult testResult = new TestResult();
				testResult.setTestId(testConfigEntry.getKey());
				testResult.setTestConfig(testConfig);
				Value js = context.eval("js", testConfig.getExpr());
				testResult.setPassed(js.asBoolean());
				LOG.info("Test <" + testConfigEntry.getKey() + "> " + (js.asBoolean() ? "Passed" : "Failed"));
				testResults.add(testResult);
			}

		}
		return testResults;
	}

}
