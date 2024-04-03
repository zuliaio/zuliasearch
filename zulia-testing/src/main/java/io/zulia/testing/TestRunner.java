package io.zulia.testing;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import io.zulia.client.command.builder.CountFacet;
import io.zulia.client.command.builder.FilterQuery;
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
import io.zulia.testing.config.QueryConfig;
import io.zulia.testing.config.StatFacetConfig;
import io.zulia.testing.config.TestConfig;
import io.zulia.testing.config.ZuliaTestConfig;
import io.zulia.testing.js.dto.DocumentProxyObject;
import io.zulia.testing.js.dto.FacetProxyObject;
import io.zulia.testing.js.dto.FacetValueObject;
import io.zulia.testing.js.dto.QueryResultObject;
import io.zulia.testing.result.TestResult;
import io.zulia.util.exception.ThrowingSupplier;
import org.bson.Document;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.proxy.ProxyObject;
import org.jetbrains.annotations.NotNull;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestRunner {

	public static void main(String[] args) throws Exception {
		Yaml yaml = new Yaml();

		try (InputStream inputStream = TestRunner.class.getResource("/testing.yaml").openStream()) {
			List<TestResult> testResults = runTests(yaml, inputStream);
			for (TestResult testResult : testResults) {
				System.out.println(testResult.getTestId() + " result is " + testResult.isPassed());
			}
		}

	}

	@NotNull
	public static List<TestResult> runTests(Yaml yaml, InputStream inputStream) throws Exception {
		ZuliaTestConfig zuliaTestConfig = yaml.loadAs(inputStream, ZuliaTestConfig.class);
		return runTests(zuliaTestConfig);
	}

	@NotNull
	public static List<TestResult> runTests(ZuliaTestConfig zuliaTestConfig) throws Exception {
		Map<String, Supplier<ZuliaWorkPool>> connectionToConnectionConfig = new HashMap<>();
		for (Map.Entry<String, ConnectionConfig> indexConfigEntry : zuliaTestConfig.getConnections().entrySet()) {
			ConnectionConfig connectionConfig = indexConfigEntry.getValue();

			ThrowingSupplier<ZuliaWorkPool> throwingSupplier = () -> {
				ZuliaPoolConfig zuliaPoolConfig = new ZuliaPoolConfig().addNode(connectionConfig.getServerAddress(), connectionConfig.getPort());
				return new ZuliaWorkPool(zuliaPoolConfig);
			};

			connectionToConnectionConfig.put(indexConfigEntry.getKey(), Suppliers.memoize(throwingSupplier));
		}

		for (Map.Entry<String, IndexConfig> indexConfigEntry : zuliaTestConfig.getIndexes().entrySet()) {
			IndexConfig indexConfig = indexConfigEntry.getValue();
			Supplier<ZuliaWorkPool> zuliaWorkPoolSupplier = connectionToConnectionConfig.get(indexConfig.getConnection());
			if (zuliaWorkPoolSupplier == null) {
				throw new IllegalArgumentException(
						"Failed to find connection config <" + indexConfig.getConnection() + "> for query <" + indexConfigEntry.getKey() + ">");
			}
		}

		Map<String, QueryResultObject> resultMap = new HashMap<>();
		for (Map.Entry<String, QueryConfig> queryConfigEntry : zuliaTestConfig.getQueries().entrySet()) {
			String queryId = queryConfigEntry.getKey();
			QueryConfig queryConfig = queryConfigEntry.getValue();
			IndexConfig indexConfig = zuliaTestConfig.getIndexes().get(queryConfig.getIndex());
			Supplier<ZuliaWorkPool> zuliaWorkPoolSupplier = connectionToConnectionConfig.get(indexConfig.getConnection());
			ZuliaWorkPool zuliaWorkPool = zuliaWorkPoolSupplier.get();

			Search s = new Search(indexConfig.getIndexName());
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

			SearchResult searchResult = zuliaWorkPool.search(s);

			QueryResultObject result = new QueryResultObject();
			result.count = searchResult.getTotalHits();

			List<ProxyObject> docs = new ArrayList<>();
			for (CompleteResult completeResult : searchResult.getCompleteResults()) {
				Document document = completeResult.getDocument();
				docs.add(new DocumentProxyObject(document));
			}
			Map<String, List<FacetValueObject>> facetResults = new HashMap<>();

			if (queryConfig.getFacets() != null) {
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
				result.facet = new FacetProxyObject(facetResults);
			}

			if (queryConfig.getStatFacets() != null) {
				for (StatFacetConfig statFacet : queryConfig.getStatFacets()) {

				}
			}

			result.doc = docs;
			resultMap.put(queryId, result);
		}

		List<TestResult> testResults = new ArrayList<>();
		try (Context context = Context.newBuilder("js").option("engine.WarnInterpreterOnly", "false").allowHostAccess(HostAccess.ALL).build()) {
			Value jsValue = context.getBindings("js");
			for (Map.Entry<String, QueryResultObject> countToEntry : resultMap.entrySet()) {
				jsValue.putMember(countToEntry.getKey(), countToEntry.getValue());
			}

			for (Map.Entry<String, TestConfig> testConfigEntry : zuliaTestConfig.getTests().entrySet()) {
				TestConfig testConfig = testConfigEntry.getValue();

				TestResult testResult = new TestResult();
				testResult.setTestId(testConfigEntry.getKey());
				testResult.setTestConfig(testConfig);
				Value js = context.eval("js", testConfig.getExpr());
				testResult.setPassed(js.asBoolean());
				testResults.add(testResult);
			}

		}
		return testResults;
	}
}
