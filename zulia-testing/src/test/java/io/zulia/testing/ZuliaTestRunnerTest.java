package io.zulia.testing;

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
import org.bson.Document;
import org.graalvm.polyglot.proxy.ProxyObject;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ZuliaTestRunnerTest {

	private ZuliaTestRunner createRunner(List<TestConfig> tests) {
		ZuliaTestConfig config = new ZuliaTestConfig();
		config.setConnections(List.of());
		config.setIndexes(List.of());
		config.setSearches(List.of());
		config.setTests(tests);
		return new ZuliaTestRunner(config);
	}

	private TestConfig createTest(String name, String expr) {
		TestConfig tc = new TestConfig();
		tc.setName(name);
		tc.setExpr(expr);
		return tc;
	}

	@Test
	public void countExpression() {
		ZuliaTestRunner runner = createRunner(List.of(
				createTest("highCount", "search1.count > 1000"),
				createTest("lowCount", "search1.count < 100")
		));

		QueryResultObject qr = new QueryResultObject();
		qr.count = 5000;

		List<TestResult> results = runner.evaluateTestsWithQueryResults(Map.of("search1", qr));

		Assertions.assertEquals(2, results.size());
		Assertions.assertTrue(results.get(0).isPassed(), "count > 1000 should pass");
		Assertions.assertFalse(results.get(1).isPassed(), "count < 100 should fail");
	}

	@Test
	public void multipleSearchResultComparison() {
		ZuliaTestRunner runner = createRunner(List.of(
				createTest("mostHaveTitle", "withoutTitle.count < allDocs.count * 0.01")
		));

		QueryResultObject allDocs = new QueryResultObject();
		allDocs.count = 100000;

		QueryResultObject withoutTitle = new QueryResultObject();
		withoutTitle.count = 500;

		Map<String, QueryResultObject> resultMap = new HashMap<>();
		resultMap.put("allDocs", allDocs);
		resultMap.put("withoutTitle", withoutTitle);

		List<TestResult> results = runner.evaluateTestsWithQueryResults(resultMap);

		Assertions.assertEquals(1, results.size());
		Assertions.assertTrue(results.getFirst().isPassed());
	}

	@Test
	public void documentFieldAccess() {
		ZuliaTestRunner runner = createRunner(List.of(
				createTest("exactMatch", "search1.count == 1"),
				createTest("checkAuthor", "search1.doc[0][\"authors\"][0][\"lastName\"] == \"Smith\""),
				createTest("checkTitle", "search1.doc[0].title == \"Test Article\"")
		));

		Document authorDoc = new Document();
		authorDoc.put("lastName", "Smith");
		authorDoc.put("firstName", "John");

		Document doc = new Document();
		doc.put("title", "Test Article");
		doc.put("authors", List.of(authorDoc));

		QueryResultObject qr = new QueryResultObject();
		qr.count = 1;
		qr.doc = new ArrayList<>();
		qr.doc.add(new DocumentProxyObject(doc));

		List<TestResult> results = runner.evaluateTestsWithQueryResults(Map.of("search1", qr));

		Assertions.assertEquals(3, results.size());
		for (TestResult result : results) {
			Assertions.assertTrue(result.isPassed(), result.getTestId() + " should pass");
		}
	}

	@Test
	public void facetAccess() {
		ZuliaTestRunner runner = createRunner(List.of(
				createTest("topYear", "search1.facet[\"pubYear\"][0].label == \"2022\" && search1.facet[\"pubYear\"][0].count > 1000"),
				createTest("secondYear", "search1.facet[\"pubYear\"][1].label == \"2021\"")
		));

		FacetValueObject fv1 = new FacetValueObject();
		fv1.label = "2022";
		fv1.count = 5000;

		FacetValueObject fv2 = new FacetValueObject();
		fv2.label = "2021";
		fv2.count = 4000;

		Map<String, List<FacetValueObject>> facetMap = new HashMap<>();
		facetMap.put("pubYear", List.of(fv1, fv2));

		QueryResultObject qr = new QueryResultObject();
		qr.count = 9000;
		qr.facet = new KeyedListProxyObject<>(facetMap);

		List<TestResult> results = runner.evaluateTestsWithQueryResults(Map.of("search1", qr));

		Assertions.assertEquals(2, results.size());
		for (TestResult result : results) {
			Assertions.assertTrue(result.isPassed(), result.getTestId() + " should pass");
		}
	}

	@Test
	public void statFacetAccess() {
		ZuliaTestRunner runner = createRunner(List.of(
				createTest("statLabel", "search1.statFacet[\"pubYear-authorCount\"][0].label == \"2022\""),
				createTest("statDocCount", "search1.statFacet[\"pubYear-authorCount\"][0].docCount > 100"),
				createTest("statSum", "search1.statFacet[\"pubYear-authorCount\"][0].sum > 5000")
		));

		StatFacetValueObject sfv = new StatFacetValueObject();
		sfv.label = "2022";
		sfv.docCount = 2000;
		sfv.allDocCount = 2000;
		sfv.valueCount = 2000;
		sfv.sum = 10000;
		sfv.max = 50;
		sfv.min = 1;

		Map<String, List<StatFacetValueObject>> statFacetMap = new HashMap<>();
		statFacetMap.put("pubYear-authorCount", List.of(sfv));

		QueryResultObject qr = new QueryResultObject();
		qr.count = 2000;
		qr.statFacet = new KeyedListProxyObject<>(statFacetMap);

		List<TestResult> results = runner.evaluateTestsWithQueryResults(Map.of("search1", qr));

		Assertions.assertEquals(3, results.size());
		for (TestResult result : results) {
			Assertions.assertTrue(result.isPassed(), result.getTestId() + " should pass");
		}
	}

	@Test
	public void numStatWithPercentiles() {
		ZuliaTestRunner runner = createRunner(List.of(
				createTest("avgCheck", "(search1.numStat[\"pubYear\"].sum / search1.numStat[\"pubYear\"].docCount) > 2008"),
				createTest("p10", "search1.numStat[\"pubYear\"].percentiles[0].value < 1996"),
				createTest("p90", "search1.numStat[\"pubYear\"].percentiles[2].value > 2021")
		));

		PercentileValueObject p10 = new PercentileValueObject();
		p10.point = 0.1;
		p10.value = 1990;

		PercentileValueObject p50 = new PercentileValueObject();
		p50.point = 0.5;
		p50.value = 2015;

		PercentileValueObject p90 = new PercentileValueObject();
		p90.point = 0.9;
		p90.value = 2023;

		StatFacetValueObject stat = new StatFacetValueObject();
		stat.docCount = 1000;
		stat.sum = 2015000;
		stat.min = 1950;
		stat.max = 2024;
		stat.percentiles = List.of(p10, p50, p90);

		Map<String, StatFacetValueObject> numStatMap = new HashMap<>();
		numStatMap.put("pubYear", stat);

		QueryResultObject qr = new QueryResultObject();
		qr.count = 1000;
		qr.numStat = new KeyedProxyObject<>(numStatMap);

		List<TestResult> results = runner.evaluateTestsWithQueryResults(Map.of("search1", qr));

		Assertions.assertEquals(3, results.size());
		for (TestResult result : results) {
			Assertions.assertTrue(result.isPassed(), result.getTestId() + " should pass");
		}
	}

	@Test
	public void failingTestReportsFailure() {
		ZuliaTestRunner runner = createRunner(List.of(
				createTest("shouldFail", "search1.count > 999")
		));

		QueryResultObject qr = new QueryResultObject();
		qr.count = 10;

		List<TestResult> results = runner.evaluateTestsWithQueryResults(Map.of("search1", qr));

		Assertions.assertEquals(1, results.size());
		Assertions.assertFalse(results.getFirst().isPassed());
		Assertions.assertEquals("shouldFail", results.getFirst().getTestId());
	}

	@Test
	public void testResultPreservesConfig() {
		TestConfig tc = createTest("myTest", "s.count == 42");
		ZuliaTestRunner runner = createRunner(List.of(tc));

		QueryResultObject qr = new QueryResultObject();
		qr.count = 42;

		List<TestResult> results = runner.evaluateTestsWithQueryResults(Map.of("s", qr));

		Assertions.assertEquals(1, results.size());
		Assertions.assertTrue(results.getFirst().isPassed());
		Assertions.assertEquals("myTest", results.getFirst().getTestId());
		Assertions.assertSame(tc, results.getFirst().getTestConfig());
	}

	@Test
	public void nestedDocumentAccess() {
		ZuliaTestRunner runner = createRunner(List.of(
				createTest("nestedField", "search1.doc[0].address.city == \"Boston\"")
		));

		Document address = new Document();
		address.put("city", "Boston");
		address.put("state", "MA");

		Document doc = new Document();
		doc.put("name", "Test");
		doc.put("address", address);

		QueryResultObject qr = new QueryResultObject();
		qr.count = 1;
		qr.doc = new ArrayList<>();
		qr.doc.add(new DocumentProxyObject(doc));

		List<TestResult> results = runner.evaluateTestsWithQueryResults(Map.of("search1", qr));

		Assertions.assertEquals(1, results.size());
		Assertions.assertTrue(results.getFirst().isPassed());
	}
}
