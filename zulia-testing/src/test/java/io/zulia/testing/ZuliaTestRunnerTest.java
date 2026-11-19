package io.zulia.testing;

import io.zulia.client.command.builder.Search;
import io.zulia.testing.config.FunctionConfig;
import io.zulia.testing.config.QueryConfig;
import io.zulia.testing.config.SearchConfig;
import io.zulia.testing.config.TestConfig;
import io.zulia.testing.config.ValueConfig;
import io.zulia.testing.config.ZuliaTestConfig;
import io.zulia.testing.js.dto.DocumentProxyObject;
import io.zulia.testing.js.dto.FacetValueObject;
import io.zulia.testing.js.dto.KeyedListProxyObject;
import io.zulia.testing.js.dto.KeyedProxyObject;
import io.zulia.testing.js.dto.PercentileValueObject;
import io.zulia.testing.js.dto.QueryResultObject;
import io.zulia.testing.js.dto.StatFacetValueObject;
import io.zulia.testing.result.TestResult;
import io.zulia.testing.result.TestRun;
import io.zulia.testing.result.TestRunReport;
import io.zulia.testing.result.TestStatus;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDate;
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

	private ZuliaTestRunner createRunner(List<TestConfig> tests, List<ValueConfig> values) {
		ZuliaTestRunner runner = createRunner(tests);
		runner.zuliaTestConfig.setValues(values);
		return runner;
	}

	private FunctionConfig createFunction(String name, List<String> args, String expr, String body) {
		FunctionConfig fc = new FunctionConfig();
		fc.setName(name);
		fc.setArgs(args);
		fc.setExpr(expr);
		fc.setBody(body);
		return fc;
	}

	private ZuliaTestRunner withFunctions(ZuliaTestRunner runner, FunctionConfig... functions) {
		runner.zuliaTestConfig.setFunctions(List.of(functions));
		return runner;
	}

	private ValueConfig createValue(String name, String expr) {
		ValueConfig vc = new ValueConfig();
		vc.setName(name);
		vc.setExpr(expr);
		return vc;
	}

	private QueryResultObject facetResult() {
		FacetValueObject fv1 = new FacetValueObject();
		fv1.label = "2022";
		fv1.count = 5000;
		FacetValueObject fv2 = new FacetValueObject();
		fv2.label = "2021";
		fv2.count = 4000;
		QueryResultObject qr = new QueryResultObject();
		qr.count = 9000;
		qr.facet = new KeyedListProxyObject<>(Map.of("pubYear", List.of(fv1, fv2)));
		return qr;
	}

	/**
	 * The message of the single test in the run, which must be an ERROR.
	 */
	private String onlyError(TestRun run) {
		TestResult result = run.testResults().getFirst();
		Assertions.assertEquals(TestStatus.ERROR, result.getStatus());
		return result.getMessage();
	}

	private TestConfig createTest(String name, String expr) {
		TestConfig tc = new TestConfig();
		tc.setName(name);
		tc.setExpr(expr);
		return tc;
	}

	@Test
	public void countExpression() {
		ZuliaTestRunner runner = createRunner(List.of(createTest("highCount", "search1.count > 1000"), createTest("lowCount", "search1.count < 100")));

		QueryResultObject qr = new QueryResultObject();
		qr.count = 5000;

		List<TestResult> results = runner.evaluateTestsWithQueryResults(Map.of("search1", qr));

		Assertions.assertEquals(2, results.size());
		Assertions.assertTrue(results.get(0).isPassed(), "count > 1000 should pass");
		Assertions.assertFalse(results.get(1).isPassed(), "count < 100 should fail");
	}

	@Test
	public void multipleSearchResultComparison() {
		ZuliaTestRunner runner = createRunner(List.of(createTest("mostHaveTitle", "withoutTitle.count < allDocs.count * 0.01")));

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
		ZuliaTestRunner runner = createRunner(
				List.of(createTest("exactMatch", "search1.count == 1"), createTest("checkAuthor", "search1.doc[0][\"authors\"][0][\"lastName\"] == \"Smith\""),
						createTest("checkTitle", "search1.doc[0].title == \"Test Article\"")));

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
		ZuliaTestRunner runner = createRunner(
				List.of(createTest("topYear", "search1.facet[\"pubYear\"][0].label == \"2022\" && search1.facet[\"pubYear\"][0].count > 1000"),
						createTest("secondYear", "search1.facet[\"pubYear\"][1].label == \"2021\"")));

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
		ZuliaTestRunner runner = createRunner(List.of(createTest("statLabel", "search1.statFacet[\"pubYear-authorCount\"][0].label == \"2022\""),
				createTest("statDocCount", "search1.statFacet[\"pubYear-authorCount\"][0].docCount > 100"),
				createTest("statSum", "search1.statFacet[\"pubYear-authorCount\"][0].sum > 5000")));

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
		ZuliaTestRunner runner = createRunner(
				List.of(createTest("avgCheck", "(search1.numStat[\"pubYear\"].sum / search1.numStat[\"pubYear\"].docCount) > 2008"),
						createTest("p10", "search1.numStat[\"pubYear\"].percentiles[0].value < 1996"),
						createTest("p90", "search1.numStat[\"pubYear\"].percentiles[2].value > 2021")));

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
		ZuliaTestRunner runner = createRunner(List.of(createTest("shouldFail", "search1.count > 999")));

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
		ZuliaTestRunner runner = createRunner(List.of(createTest("nestedField", "search1.doc[0].address.city == \"Boston\"")));

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

	@Test
	public void valuesAreRecordedAsPlainJava() {
		ZuliaTestRunner runner = createRunner(List.of(createTest("count", "search1.count > 0")),
				List.of(createValue("total", "search1.count"), createValue("ratio", "search1.count / 4"),
						createValue("topLabel", "search1.facet[\"pubYear\"][0].label"), createValue("flag", "search1.count > 100"),
						createValue("topFacet", "search1.facet[\"pubYear\"][0]"), createValue("labels", "search1.facet[\"pubYear\"].map(f => f.label)"),
						createValue("nothing", "null")));

		TestRun run = runner.evaluate(Instant.now(), Map.of("search1", facetResult()));

		Map<String, Object> values = run.values();
		Assertions.assertEquals(9000L, values.get("total"));
		Assertions.assertEquals(2250L, values.get("ratio"));
		Assertions.assertEquals("2022", values.get("topLabel"));
		Assertions.assertEquals(true, values.get("flag"));
		Assertions.assertEquals(Map.of("label", "2022", "count", 5000L), values.get("topFacet"));
		Assertions.assertEquals(List.of("2022", "2021"), values.get("labels"));
		Assertions.assertTrue(values.containsKey("nothing"));
		Assertions.assertNull(values.get("nothing"));
		Assertions.assertEquals(List.of("total", "ratio", "topLabel", "flag", "topFacet", "labels", "nothing"), new ArrayList<>(values.keySet()),
				"values keep config order");
		Assertions.assertTrue(run.testResults().getFirst().isPassed());
	}

	@Test
	public void fractionalValueStaysDouble() {
		ZuliaTestRunner runner = createRunner(List.of(), List.of(createValue("avg", "search1.count / 7")));
		QueryResultObject qr = new QueryResultObject();
		qr.count = 10;
		TestRun run = runner.evaluate(Instant.now(), Map.of("search1", qr));
		Assertions.assertEquals(10.0 / 7, (Double) run.values().get("avg"), 1e-12);
	}

	@Test
	public void savedValuesAreAvailableToExpressions() {
		ZuliaTestRunner runner = createRunner(
				List.of(createTest("grewSinceLastRun", "search1.count > saved.total"), createTest("bracketAccess", "search1.count > saved[\"total\"] * 0.5"),
						createTest("nestedSaved", "saved.topFacet.label == \"2021\" && saved.labels[1] == \"2020\"")),
				List.of(createValue("total", "search1.count")));
		runner.setSavedValues(Map.of("total", 8000L, "topFacet", Map.of("label", "2021", "count", 4000L), "labels", List.of("2021", "2020")));

		TestRun run = runner.evaluate(Instant.now(), Map.of("search1", facetResult()));

		for (TestResult result : run.testResults()) {
			Assertions.assertTrue(result.isPassed(), result.getTestId() + " should pass");
		}
		Assertions.assertEquals(9000L, run.values().get("total"));
	}

	@Test
	public void savedReadWithoutSavedValuesIsAnErrorForThatTestOnly() {
		ZuliaTestRunner runner = createRunner(List.of(createTest("needsSaved", "search1.count > saved.total"), createTest("plain", "search1.count > 0"),
				createTest("wordInAString", "\"saved\" == \"saved\"")));
		QueryResultObject qr = new QueryResultObject();
		qr.count = 10;

		TestRun run = runner.evaluate(Instant.now(), Map.of("search1", qr));

		Assertions.assertEquals(List.of(TestStatus.ERROR, TestStatus.PASS, TestStatus.PASS), run.testResults().stream().map(TestResult::getStatus).toList());
		String message = run.testResults().getFirst().getMessage();
		Assertions.assertTrue(message.contains("test <needsSaved>"), message);
		Assertions.assertTrue(message.contains("Reads saved.total but no saved values were loaded"), message);
		Assertions.assertTrue(message.contains("--recordBaseline"), message);
	}

	@Test
	public void searchNamedSavedStillWorksWhenNoReportIsLoaded() {
		ZuliaTestRunner runner = createRunner(List.of(createTest("t", "saved.count == 10")));
		QueryResultObject qr = new QueryResultObject();
		qr.count = 10;

		Assertions.assertTrue(runner.evaluate(Instant.now(), Map.of("saved", qr)).testResults().getFirst().isPassed());
	}

	@Test
	public void baselineRunSkipsWhatReadsSavedAndRecordsTheRest() {
		ZuliaTestRunner runner = withFunctions(createRunner(
						List.of(createTest("plain", "search1.count > 0"), createTest("direct", "search1.count > saved.total"), createTest("throughFunction", "grew()"),
								createTest("failing", "search1.count < 0")),
						List.of(createValue("total", "search1.count"), createValue("growth", "search1.count - saved.total"))),
				createFunction("grew", List.of(), "search1.count > saved.total", null));
		runner.recordBaseline();

		TestRun run = runner.evaluate(Instant.now(), Map.of("search1", facetResult()));

		Assertions.assertEquals(List.of(TestStatus.PASS, TestStatus.SKIPPED, TestStatus.SKIPPED, TestStatus.FAIL),
				run.testResults().stream().map(TestResult::getStatus).toList());
		Assertions.assertTrue(run.testResults().get(1).getMessage().contains("saved.total"), run.testResults().get(1).getMessage());
		Assertions.assertTrue(run.testResults().get(1).getMessage().contains("recording a baseline"), run.testResults().get(1).getMessage());
		Assertions.assertEquals(Map.of("total", 9000L), run.values());
		Assertions.assertEquals(List.of("growth"), run.unrecordedValues().stream().map(TestRunReport.Outcome::name).toList());
		Assertions.assertEquals(TestStatus.SKIPPED, run.unrecordedValues().getFirst().status());
		Assertions.assertTrue(run.anyFailed());
		Assertions.assertFalse(run.anyErrors(), "a skipped test is not an error");

		ZuliaTestRunner next = createRunner(List.of(createTest("direct", "search1.count >= saved.total")));
		next.setSavedValues(run.toReport().values());
		Assertions.assertTrue(next.evaluateTestsWithQueryResults(Map.of("search1", facetResult())).getFirst().isPassed());
	}

	@Test
	public void valueSkippedByThePreviousRunIsSkippedAgainThenAvailable() {
		List<TestConfig> tests = List.of(createTest("tracks", "search1.count >= saved.total"), createTest("growthHolds", "saved.growth >= 0"));
		List<ValueConfig> values = List.of(createValue("total", "search1.count"), createValue("growth", "search1.count - saved.total"));

		TestRun first = createRunner(tests, values).recordBaseline().evaluate(Instant.now(), Map.of("search1", facetResult()));
		Assertions.assertEquals(List.of(TestStatus.SKIPPED, TestStatus.SKIPPED), first.testResults().stream().map(TestResult::getStatus).toList());

		TestRun second = createRunner(tests, values).setPreviousReport(first.toReport()).evaluate(Instant.now(), Map.of("search1", facetResult()));
		Assertions.assertEquals(List.of(TestStatus.PASS, TestStatus.SKIPPED), second.testResults().stream().map(TestResult::getStatus).toList());
		Assertions.assertTrue(second.testResults().get(1).getMessage().contains("saved.growth, which the previous run skipped"),
				second.testResults().get(1).getMessage());
		Assertions.assertEquals(0L, second.values().get("growth"));
		Assertions.assertFalse(second.anyErrors());

		TestRun third = createRunner(tests, values).setPreviousReport(second.toReport()).evaluate(Instant.now(), Map.of("search1", facetResult()));
		Assertions.assertEquals(List.of(TestStatus.PASS, TestStatus.PASS), third.testResults().stream().map(TestResult::getStatus).toList());
	}

	@Test
	public void missingSavedKeyIsAnErrorThatListsTheSavedNames() {
		ZuliaTestRunner runner = createRunner(List.of(createTest("ok", "search1.count > 0"), createTest("typo", "search1.count > saved.totl")));
		runner.setSavedValues(Map.of("total", 5L));
		QueryResultObject qr = new QueryResultObject();
		qr.count = 10;

		TestRun run = runner.evaluate(Instant.now(), Map.of("search1", qr));

		Assertions.assertEquals(List.of(TestStatus.PASS, TestStatus.ERROR), run.testResults().stream().map(TestResult::getStatus).toList());
		String message = run.testResults().get(1).getMessage();
		Assertions.assertTrue(message.contains("Saved value <totl> was not found"), message);
		Assertions.assertTrue(message.contains("[total]"), message);
	}

	@Test
	public void missingSavedKeyThroughDynamicAccessIsAnError() {
		ZuliaTestRunner runner = createRunner(List.of(createTest("dynamic", "search1.count > saved[\"tot\" + \"l\"]")));
		runner.setSavedValues(Map.of("total", 5L));
		QueryResultObject qr = new QueryResultObject();
		qr.count = 10;

		String message = onlyError(runner.evaluate(Instant.now(), Map.of("search1", qr)));
		Assertions.assertTrue(message.contains("test <dynamic>"), message);
		Assertions.assertTrue(message.contains("Saved value <totl> was not found"), message);
	}

	@Test
	public void nonBooleanTestExpressionIsAnErrorAndTheRunContinues() {
		ZuliaTestRunner runner = createRunner(
				List.of(createTest("before", "search1.count > 0"), createTest("notBoolean", "search1.count"), createTest("after", "search1.count < 0")),
				List.of(createValue("total", "search1.count")));
		QueryResultObject qr = new QueryResultObject();
		qr.count = 10;

		TestRun run = runner.evaluate(Instant.now(), Map.of("search1", qr));

		Assertions.assertEquals(List.of(TestStatus.PASS, TestStatus.ERROR, TestStatus.FAIL), run.testResults().stream().map(TestResult::getStatus).toList());
		Assertions.assertTrue(run.testResults().get(1).getMessage().contains("must evaluate to a boolean but returned <10>"),
				run.testResults().get(1).getMessage());
		Assertions.assertFalse(run.testResults().get(1).isPassed());
		Assertions.assertNull(run.testResults().get(0).getMessage());
		Assertions.assertEquals(10L, run.values().get("total"));
		Assertions.assertTrue(run.anyErrors());
		Assertions.assertTrue(run.anyFailed());
	}

	@Test
	public void reportRoundTripsThroughJson() throws Exception {
		ZuliaTestRunner runner = createRunner(List.of(createTest("pass", "search1.count > 0"), createTest("fail", "search1.count < 0")),
				List.of(createValue("total", "search1.count"), createValue("topFacet", "search1.facet[\"pubYear\"][0]")));
		runner.setAsOf(LocalDate.of(2026, 10, 22));
		TestRun run = runner.evaluate(Instant.parse("2026-10-22T14:00:00Z"), Map.of("search1", facetResult()));
		Assertions.assertTrue(run.anyFailed());

		Path file = Files.createTempFile("zuliatest", ".json");
		try {
			run.toReport().write(file);
			TestRunReport loaded = TestRunReport.read(file);

			Assertions.assertEquals(TestRunReport.CURRENT_FORMAT_VERSION, loaded.formatVersion());
			Assertions.assertEquals("2026-10-22T14:00:00Z", loaded.timestamp());
			Assertions.assertEquals("2026-10-22", loaded.asOf());
			Assertions.assertEquals(9000L, loaded.values().get("total"));
			Assertions.assertEquals(Map.of("label", "2022", "count", 5000L), loaded.values().get("topFacet"));
			Assertions.assertEquals(List.of(new TestRunReport.Outcome("pass", TestStatus.PASS, null), new TestRunReport.Outcome("fail", TestStatus.FAIL, null)),
					loaded.tests());
			Assertions.assertEquals(List.of(), loaded.unrecordedValues());

			ZuliaTestRunner next = createRunner(
					List.of(createTest("sameAsSaved", "search1.count == saved.total && search1.facet[\"pubYear\"][0].count == saved.topFacet.count")));
			next.setSavedValues(loaded.values());
			Assertions.assertTrue(next.evaluateTestsWithQueryResults(Map.of("search1", facetResult())).getFirst().isPassed());
		}
		finally {
			Files.deleteIfExists(file);
		}
	}

	@Test
	public void reportKeepsAngleBracketsReadable() {
		ZuliaTestRunner runner = createRunner(List.of(createTest("notBoolean", "search1.count")), List.of());
		TestRun run = runner.evaluate(Instant.parse("2026-10-22T14:00:00Z"), Map.of("search1", facetResult()));

		String json = run.toReport().toJson();
		Assertions.assertTrue(json.contains("Test <notBoolean> expression <search1.count> must evaluate to a boolean"), json);
		Assertions.assertFalse(json.contains("\\u003c"), json);
	}

	@Test
	public void templatesResolveInQueries() {
		ZuliaTestRunner runner = createRunner(List.of()).setAsOf(LocalDate.of(2026, 10, 22));

		QueryConfig thisYear = new QueryConfig();
		thisYear.setQ("pubDate:[${startOfYear} TO ${today}]");
		QueryConfig lastYear = new QueryConfig();
		lastYear.setQ("pubDate:[${now-1y/y} TO ${now-1y}]");
		lastYear.setQueryType(io.zulia.message.ZuliaQuery.Query.QueryType.FILTER);
		QueryConfig plain = new QueryConfig();
		plain.setQ("title:cancer");
		SearchConfig searchConfig = new SearchConfig();
		searchConfig.setName("s");
		searchConfig.setIndex("pubs");
		searchConfig.setQueries(List.of(thisYear, lastYear, plain));

		Search search = runner.buildSearch("publications", searchConfig);
		List<String> queries = search.getRequest().getQueryList().stream().map(io.zulia.message.ZuliaQuery.Query::getQ).toList();
		Assertions.assertEquals(List.of("pubDate:[2026-01-01 TO 2026-10-22]", "pubDate:[2025-01-01 TO 2025-10-22]", "title:cancer"), queries);
	}

	@Test
	public void templatesResolveInExpressions() {
		ZuliaTestRunner runner = createRunner(List.of(createTest("topYearIsLastYear", "search1.facet[\"pubYear\"][0].label == \"${oneYearAgo|yyyy}\"")),
				List.of(createValue("asOf", "\"${today}\"")));
		runner.setAsOf(LocalDate.of(2023, 6, 1));

		TestRun run = runner.evaluate(Instant.now(), Map.of("search1", facetResult()));

		Assertions.assertTrue(run.testResults().getFirst().isPassed());
		Assertions.assertEquals("2023-06-01", run.values().get("asOf"));
		Assertions.assertEquals(LocalDate.of(2023, 6, 1), run.asOf());
	}

	@Test
	public void unknownTemplateVariableFailsWithContext() {
		ZuliaTestRunner runner = createRunner(List.of(createTest("bad", "search1.count > ${nope}")));
		QueryResultObject qr = new QueryResultObject();
		qr.count = 1;
		String message = onlyError(runner.evaluate(Instant.now(), Map.of("search1", qr)));
		Assertions.assertTrue(message.contains("<nope>"), message);
	}

	@Test
	public void functionsAreSharedAcrossTestsAndValues() {
		ZuliaTestRunner runner = createRunner(List.of(createTest("withinTolerance", "within(search1.count, 10000, 0.1)"),
						createTest("outsideTolerance", "!within(search1.count, 10000, 0.01)"),
						createTest("topLabelViaBody", "topLabel(search1, \"pubYear\") == \"2022\""), createTest("noArgs", "nine() == 9000")),
				List.of(createValue("top", "topLabel(search1, \"pubYear\")"), createValue("missing", "topLabel(search1, \"nope\")")));
		withFunctions(runner, createFunction("within", List.of("actual", "expected", "tolerance"), "Math.abs(actual - expected) <= expected * tolerance", null),
				createFunction("topLabel", List.of("search", "field"), null,
						"const values = search.facet[field];\nif (values === undefined) { return null; }\nreturn values[0].label;"),
				createFunction("nine", null, "search1.count", null));

		TestRun run = runner.evaluate(Instant.now(), Map.of("search1", facetResult()));

		for (TestResult result : run.testResults()) {
			Assertions.assertTrue(result.isPassed(), result.getTestId() + " should pass");
		}
		Assertions.assertEquals("2022", run.values().get("top"));
		Assertions.assertNull(run.values().get("missing"));
	}

	@Test
	public void functionsCanUseSavedValuesAndTemplates() {
		ZuliaTestRunner runner = createRunner(List.of(createTest("grew", "grewSince(\"total\", search1.count)"), createTest("year", "lastYear() == \"2025\"")));
		withFunctions(runner, createFunction("grewSince", List.of("name", "current"), "current > saved[name]", null),
				createFunction("lastYear", List.of(), "\"${oneYearAgo|yyyy}\"", null));
		runner.setSavedValues(Map.of("total", 100L)).setAsOf(LocalDate.of(2026, 10, 22));

		TestRun run = runner.evaluate(Instant.now(), Map.of("search1", facetResult()));
		for (TestResult result : run.testResults()) {
			Assertions.assertTrue(result.isPassed(), result.getTestId() + " should pass");
		}
	}

	@Test
	public void functionReadingMissingSavedNameIsAnErrorForItsCaller() {
		ZuliaTestRunner runner = createRunner(List.of(createTest("t", "f() > 0")));
		withFunctions(runner, createFunction("f", List.of(), "saved.nope", null));
		runner.setSavedValues(Map.of("total", 100L));
		QueryResultObject qr = new QueryResultObject();
		qr.count = 1;

		String message = onlyError(runner.evaluate(Instant.now(), Map.of("search1", qr)));
		Assertions.assertTrue(message.contains("test <t>"), message);
		Assertions.assertTrue(message.contains("Saved value <nope> was not found"), message);
	}

	@Test
	public void functionExprMayEndInALineComment() {
		ZuliaTestRunner runner = withFunctions(createRunner(List.of(createTest("t", "big(search1.count)"))),
				createFunction("big", List.of("n"), "n > 5 // more than five", null));
		QueryResultObject qr = new QueryResultObject();
		qr.count = 10;

		Assertions.assertTrue(runner.evaluate(Instant.now(), Map.of("search1", qr)).testResults().getFirst().isPassed());
	}

	@Test
	public void oneNameCannotBelongToAVariableAndAFunction() {
		ZuliaTestRunner runner = withFunctions(createRunner(List.of(createTest("t", "true"))), createFunction("limit", List.of(), "1", null));
		runner.zuliaTestConfig.setVariables(new java.util.LinkedHashMap<>(Map.of("limit", 5)));

		IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class, () -> runner.buildSearch("pubs", new SearchConfig()));
		Assertions.assertTrue(e.getMessage().contains("Name <limit> is used by a variable and a function"), e.getMessage());
	}

	@Test
	public void unexpectedFailuresAreErrorsAndTheRunContinues() {
		ZuliaTestRunner runner = createRunner(List.of(createTest("hugeOffset", "\"${now-9999999999y}\" != \"\""), createTest("after", "search1.count > 0")),
				List.of(createValue("bigInt", "10n ** 30n"), createValue("total", "search1.count")));
		QueryResultObject qr = new QueryResultObject();
		qr.count = 10;

		TestRun run = runner.evaluate(Instant.now(), Map.of("search1", qr));

		Assertions.assertEquals(List.of(TestStatus.ERROR, TestStatus.PASS), run.testResults().stream().map(TestResult::getStatus).toList());
		Assertions.assertTrue(run.testResults().getFirst().getMessage().contains("out of range"), run.testResults().getFirst().getMessage());
		Assertions.assertEquals(Map.of("total", 10L), run.values());
		Assertions.assertEquals("bigInt", run.unrecordedValues().getFirst().name());
		Assertions.assertTrue(run.unrecordedValues().getFirst().message().contains("too large to record exactly"), run.unrecordedValues().getFirst().message());
	}

	@Test
	public void configWithValuesAndNoTestsRecordsTheValues() {
		ZuliaTestRunner runner = createRunner(null, List.of(createValue("total", "search1.count")));
		QueryResultObject qr = new QueryResultObject();
		qr.count = 10;

		TestRun run = runner.evaluate(Instant.now(), Map.of("search1", qr));

		Assertions.assertEquals(List.of(), run.testResults());
		Assertions.assertEquals(Map.of("total", 10L), run.values());
	}

	@Test
	public void overriddenHooksAreUsedByRunAndRunTests() throws Exception {
		List<String> calls = new ArrayList<>();
		ZuliaTestConfig config = new ZuliaTestConfig();
		config.setConnections(List.of());
		config.setIndexes(List.of());
		config.setSearches(List.of());
		config.setTests(List.of(createTest("t", "search1.count == 7")));
		ZuliaTestRunner runner = new ZuliaTestRunner(config) {
			@Override
			protected Map<String, QueryResultObject> buildAndRunQueries() {
				calls.add("buildAndRunQueries");
				QueryResultObject qr = new QueryResultObject();
				qr.count = 7;
				return Map.of("search1", qr);
			}

			@Override
			protected List<TestResult> evaluateTestsWithQueryResults(Map<String, QueryResultObject> resultMap) {
				calls.add("evaluateTestsWithQueryResults");
				return super.evaluateTestsWithQueryResults(resultMap);
			}
		};

		Assertions.assertTrue(runner.run().testResults().getFirst().isPassed());
		Assertions.assertTrue(runner.runTests().getFirst().isPassed());
		Assertions.assertEquals(List.of("buildAndRunQueries", "buildAndRunQueries", "evaluateTestsWithQueryResults"), calls);
	}

	@Test
	public void invalidFunctionsAreRejectedDescriptively() {
		QueryResultObject qr = new QueryResultObject();
		qr.count = 1;
		Map<String, QueryResultObject> results = Map.of("search1", qr);

		ZuliaTestRunner both = withFunctions(createRunner(List.of()), createFunction("f", List.of(), "1", "return 1;"));
		Assertions.assertTrue(Assertions.assertThrows(IllegalArgumentException.class, () -> both.evaluate(Instant.now(), results)).getMessage()
				.contains("exactly one of expr"));

		ZuliaTestRunner neither = withFunctions(createRunner(List.of()), createFunction("f", List.of(), null, null));
		Assertions.assertTrue(Assertions.assertThrows(IllegalArgumentException.class, () -> neither.evaluate(Instant.now(), results)).getMessage()
				.contains("exactly one of expr"));

		ZuliaTestRunner badName = withFunctions(createRunner(List.of()), createFunction("my-func", List.of(), "1", null));
		Assertions.assertTrue(
				Assertions.assertThrows(IllegalArgumentException.class, () -> badName.evaluate(Instant.now(), results)).getMessage().contains("<my-func>"));

		ZuliaTestRunner badArg = withFunctions(createRunner(List.of()), createFunction("f", List.of("a b"), "1", null));
		Assertions.assertTrue(
				Assertions.assertThrows(IllegalArgumentException.class, () -> badArg.evaluate(Instant.now(), results)).getMessage().contains("argument <a b>"));

		ZuliaTestRunner clashesWithSearch = withFunctions(createRunner(List.of()), createFunction("search1", List.of(), "1", null));
		Assertions.assertTrue(Assertions.assertThrows(IllegalArgumentException.class, () -> clashesWithSearch.evaluate(Instant.now(), results)).getMessage()
				.contains("already taken"));

		ZuliaTestRunner clashesWithBuiltin = withFunctions(createRunner(List.of()), createFunction("Math", List.of(), "1", null));
		Assertions.assertTrue(Assertions.assertThrows(IllegalArgumentException.class, () -> clashesWithBuiltin.evaluate(Instant.now(), results)).getMessage()
				.contains("already taken"));

		ZuliaTestRunner duplicate = withFunctions(createRunner(List.of()), createFunction("f", List.of(), "1", null),
				createFunction("f", List.of(), "2", null));
		Assertions.assertTrue(Assertions.assertThrows(IllegalArgumentException.class, () -> duplicate.evaluate(Instant.now(), results)).getMessage()
				.contains("used more than once"));

		ZuliaTestRunner syntaxError = withFunctions(createRunner(List.of()), createFunction("f", List.of(), "1 +", null));
		IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class, () -> syntaxError.evaluate(Instant.now(), results));
		Assertions.assertTrue(e.getMessage().contains("function <f>"), e.getMessage());
	}

	@Test
	public void functionRuntimeErrorNamesTheTest() {
		ZuliaTestRunner runner = withFunctions(createRunner(List.of(createTest("boom", "explode() > 0"))),
				createFunction("explode", List.of(), null, "throw new Error(\"kaboom\");"));
		QueryResultObject qr = new QueryResultObject();
		qr.count = 1;
		String message = onlyError(runner.evaluate(Instant.now(), Map.of("search1", qr)));
		Assertions.assertTrue(message.contains("test <boom>"), message);
		Assertions.assertTrue(message.contains("kaboom"), message);
	}

	@Test
	public void variablesAreGlobalsAndTemplateValues() {
		ZuliaTestRunner runner = createRunner(List.of(createTest("numberVariable", "search1.count > threshold"),
				createTest("stringVariable", "search1.facet[\"pubYear\"][0].label == topYear"),
				createTest("listVariable", "years.length == 2 && years[1] == \"2021\""),
				createTest("mapVariable", "limits.min < search1.count && search1.count < limits.max"),
				createTest("templatedVariable", "lastYearRange == \"[2025-01-01 TO 2025-10-22]\""),
				createTest("variableChain", "query == \"pubDate:[2025-01-01 TO 2025-10-22]\"")), List.of(createValue("rangeUsed", "query")));
		Map<String, Object> variables = new java.util.LinkedHashMap<>();
		variables.put("threshold", 100);
		variables.put("topYear", "2022");
		variables.put("years", List.of("2022", "2021"));
		variables.put("limits", Map.of("min", 1, "max", 1000000));
		variables.put("lastYearRange", "[${startOfLastYear} TO ${oneYearAgo}]");
		variables.put("query", "pubDate:${lastYearRange}");
		runner.zuliaTestConfig.setVariables(variables);
		runner.setAsOf(LocalDate.of(2026, 10, 22));

		TestRun run = runner.evaluate(Instant.now(), Map.of("search1", facetResult()));
		for (TestResult result : run.testResults()) {
			Assertions.assertTrue(result.isPassed(), result.getTestId() + " should pass");
		}
		Assertions.assertEquals("pubDate:[2025-01-01 TO 2025-10-22]", run.values().get("rangeUsed"));
	}

	@Test
	public void variablesSubstituteIntoQueries() {
		ZuliaTestRunner runner = createRunner(List.of()).setAsOf(LocalDate.of(2026, 10, 22));
		runner.zuliaTestConfig.setVariables(Map.of("field", "pubDate", "range", "[${startOfYear} TO ${today}]"));
		QueryConfig query = new QueryConfig();
		query.setQ("${field}:${range}");
		SearchConfig searchConfig = new SearchConfig();
		searchConfig.setName("s");
		searchConfig.setIndex("pubs");
		searchConfig.setQueries(List.of(query));

		Search search = runner.buildSearch("publications", searchConfig);
		Assertions.assertEquals("pubDate:[2026-01-01 TO 2026-10-22]", search.getRequest().getQuery(0).getQ());
	}

	@Test
	public void variableOverridesReplaceAndAdd() {
		ZuliaTestRunner runner = createRunner(List.of(createTest("overridden", "threshold == 5"), createTest("added", "extra == \"x\"")));
		runner.zuliaTestConfig.setVariables(Map.of("threshold", 100));
		runner.setVariableOverrides(Map.of("threshold", 5, "extra", "x"));
		QueryResultObject qr = new QueryResultObject();
		qr.count = 1;
		for (TestResult result : runner.evaluate(Instant.now(), Map.of("search1", qr)).testResults()) {
			Assertions.assertTrue(result.isPassed(), result.getTestId() + " should pass");
		}
	}

	@Test
	public void invalidVariablesAreRejected() {
		QueryResultObject qr = new QueryResultObject();
		qr.count = 1;
		Map<String, QueryResultObject> results = Map.of("search1", qr);

		ZuliaTestRunner reserved = createRunner(List.of());
		reserved.zuliaTestConfig.setVariables(Map.of("today", "x"));
		Assertions.assertTrue(
				Assertions.assertThrows(IllegalArgumentException.class, () -> reserved.evaluate(Instant.now(), results)).getMessage().contains("reserved"));

		ZuliaTestRunner savedName = createRunner(List.of());
		savedName.zuliaTestConfig.setVariables(Map.of("saved", "x"));
		Assertions.assertTrue(
				Assertions.assertThrows(IllegalArgumentException.class, () -> savedName.evaluate(Instant.now(), results)).getMessage().contains("reserved"));

		ZuliaTestRunner badName = createRunner(List.of());
		badName.zuliaTestConfig.setVariables(Map.of("my var", "x"));
		Assertions.assertTrue(
				Assertions.assertThrows(IllegalArgumentException.class, () -> badName.evaluate(Instant.now(), results)).getMessage().contains("<my var>"));

		ZuliaTestRunner clash = createRunner(List.of());
		clash.zuliaTestConfig.setVariables(Map.of("search1", "x"));
		Assertions.assertTrue(
				Assertions.assertThrows(IllegalArgumentException.class, () -> clash.evaluate(Instant.now(), results)).getMessage().contains("already taken"));

		ZuliaTestRunner formatted = createRunner(List.of());
		Map<String, Object> ordered = new java.util.LinkedHashMap<>();
		ordered.put("a", "x");
		ordered.put("b", "${a|yyyy}");
		formatted.zuliaTestConfig.setVariables(ordered);
		Assertions.assertTrue(Assertions.assertThrows(IllegalArgumentException.class, () -> formatted.evaluate(Instant.now(), results)).getMessage()
				.contains("formats apply to dates only"));
	}

	@Test
	public void scriptDeclaresGlobalsForFunctionsTestsAndValues() {
		ZuliaTestRunner runner = createRunner(
				List.of(createTest("constFromScript", "search1.count < MILLION"), createTest("functionFromScript", "ratio(search1.count, MILLION) < 0.01"),
						createTest("dateFromScript", "lastYear == \"2025\""),
						createTest("dateFunctionInline", "date(\"now-1y/y\") == \"2025-01-01\" && date(\"oneYearAgo\", \"yyyy\") == \"2025\""),
						createTest("configFunctionUsesScript", "double(search1.count) == 18000")), List.of(createValue("million", "MILLION")));
		runner.zuliaTestConfig.setScript("""
				const MILLION = 1000000;
				const lastYear = date("oneYearAgo", "yyyy");
				function ratio(a, b) { return b === 0 ? 0 : a / b; }
				""");
		withFunctions(runner, createFunction("double", List.of("n"), "ratio(n, 1) * 2", null));
		runner.setAsOf(LocalDate.of(2026, 10, 22));

		TestRun run = runner.evaluate(Instant.now(), Map.of("search1", facetResult()));
		for (TestResult result : run.testResults()) {
			Assertions.assertTrue(result.isPassed(), result.getTestId() + " should pass");
		}
		Assertions.assertEquals(1000000L, run.values().get("million"));
	}

	@Test
	public void scriptErrorsAreDescriptive() {
		QueryResultObject qr = new QueryResultObject();
		qr.count = 1;
		Map<String, QueryResultObject> results = Map.of("search1", qr);

		ZuliaTestRunner syntax = createRunner(List.of());
		syntax.zuliaTestConfig.setScript("const x = ;");
		IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class, () -> syntax.evaluate(Instant.now(), results));
		Assertions.assertTrue(e.getMessage().contains("script <inline>"), e.getMessage());

		ZuliaTestRunner badDate = createRunner(List.of());
		badDate.zuliaTestConfig.setScript("const d = date(\"tomorrow\");");
		e = Assertions.assertThrows(IllegalArgumentException.class, () -> badDate.evaluate(Instant.now(), results));
		Assertions.assertTrue(e.getMessage().contains("<tomorrow>"), e.getMessage());

		ZuliaTestRunner badArgs = createRunner(List.of());
		badArgs.zuliaTestConfig.setScript("const d = date(5);");
		e = Assertions.assertThrows(IllegalArgumentException.class, () -> badArgs.evaluate(Instant.now(), results));
		Assertions.assertTrue(e.getMessage().contains("date() takes"), e.getMessage());
	}

	@Test
	public void searchNamesCannotShadowBuiltinsOrReservedGlobals() {
		QueryResultObject qr = new QueryResultObject();
		qr.count = 1;

		ZuliaTestRunner json = createRunner(List.of(createTest("t", "true")));
		IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class, () -> json.evaluate(Instant.now(), Map.of("JSON", qr)));
		Assertions.assertTrue(e.getMessage().contains("Search name <JSON> is already taken"), e.getMessage());

		ZuliaTestRunner date = createRunner(List.of(createTest("t", "true")));
		e = Assertions.assertThrows(IllegalArgumentException.class, () -> date.evaluate(Instant.now(), Map.of("date", qr)));
		Assertions.assertTrue(e.getMessage().contains("<date> is already taken by a search result"), e.getMessage());

		ZuliaTestRunner saved = createRunner(List.of(createTest("t", "true"))).setSavedValues(Map.of("x", 1L));
		e = Assertions.assertThrows(IllegalArgumentException.class, () -> saved.evaluate(Instant.now(), Map.of("saved", qr)));
		Assertions.assertTrue(e.getMessage().contains("<saved> is already taken by a search result"), e.getMessage());
	}

	@Test
	public void yamlDateVariablesBecomeIsoText() {
		ZuliaTestRunner runner = createRunner(
				List.of(createTest("dateOnly", "cutoff == \"2024-06-01\""), createTest("dateTime", "at == \"2024-06-01T10:30:00Z\"")),
				List.of(createValue("range", "\"[${cutoff} TO ${at}]\"")));
		Map<String, Object> variables = new java.util.LinkedHashMap<>();
		variables.put("cutoff", java.util.Date.from(Instant.parse("2024-06-01T00:00:00Z")));
		variables.put("at", java.util.Date.from(Instant.parse("2024-06-01T10:30:00Z")));
		runner.zuliaTestConfig.setVariables(variables);
		QueryResultObject qr = new QueryResultObject();
		qr.count = 1;

		TestRun run = runner.evaluate(Instant.now(), Map.of("search1", qr));
		for (TestResult result : run.testResults()) {
			Assertions.assertTrue(result.isPassed(), result.getTestId() + " should pass");
		}
		Assertions.assertEquals("[2024-06-01 TO 2024-06-01T10:30:00Z]", run.values().get("range"));
	}

	@Test
	public void functionBodiesKeepJavaScriptTemplateLiterals() {
		ZuliaTestRunner runner = withFunctions(createRunner(List.of(createTest("literal", "label(5) == \"n=5\""))),
				createFunction("label", List.of("n"), null, "const s = `n=${n}`;\nreturn s;"));
		QueryResultObject qr = new QueryResultObject();
		qr.count = 1;
		Assertions.assertTrue(runner.evaluate(Instant.now(), Map.of("search1", qr)).testResults().getFirst().isPassed());
	}

	@Test
	public void malformedPreviousReportNamesTheFile() throws Exception {
		Path file = Files.createTempFile("zuliatest", ".json");
		try {
			Files.writeString(file, "{ not json");
			java.io.IOException e = Assertions.assertThrows(java.io.IOException.class, () -> TestRunReport.read(file));
			Assertions.assertTrue(e.getMessage().contains(file.toString()), e.getMessage());

			Files.writeString(file, "{ \"timestamp\": \"x\" }");
			e = Assertions.assertThrows(java.io.IOException.class, () -> TestRunReport.read(file));
			Assertions.assertTrue(e.getMessage().contains("values section"), e.getMessage());
		}
		finally {
			Files.deleteIfExists(file);
		}
	}

	@Test
	public void undefinedOrUnrecordableValuesAreLeftOutAndReported() {
		QueryResultObject qr = facetResult();
		ZuliaTestRunner runner = createRunner(List.of(createTest("ok", "search1.count > 0")),
				List.of(createValue("typo", "search1.cuont"), createValue("f", "JSON.stringify"), createValue("ratio", "search1.count / 0"),
						createValue("nothing", "null"), createValue("total", "search1.count")));

		TestRun run = runner.evaluate(Instant.now(), Map.of("search1", qr));

		Assertions.assertEquals(List.of("nothing", "total"), List.copyOf(run.values().keySet()));
		Assertions.assertNull(run.values().get("nothing"));
		Assertions.assertEquals(List.of("typo", "f", "ratio"), run.unrecordedValues().stream().map(TestRunReport.Outcome::name).toList());
		Assertions.assertTrue(run.unrecordedValues().stream().allMatch(v -> v.status() == TestStatus.ERROR));
		Assertions.assertTrue(run.unrecordedValues().get(0).message().contains("Value <typo> evaluated to undefined"), run.unrecordedValues().get(0).message());
		Assertions.assertTrue(run.unrecordedValues().get(1).message().contains("Value <f> is a function"), run.unrecordedValues().get(1).message());
		Assertions.assertTrue(run.unrecordedValues().get(2).message().contains("Value <ratio> is Infinity"), run.unrecordedValues().get(2).message());
		Assertions.assertTrue(run.anyErrors(), "an unrecorded value is an error even when every test passes");
		Assertions.assertFalse(run.anyFailed());
		Assertions.assertEquals(run.unrecordedValues(), run.toReport().unrecordedValues());
	}

	@Test
	public void duplicateNamesAreRejected() {
		QueryResultObject qr = new QueryResultObject();
		qr.count = 1;
		Map<String, QueryResultObject> results = Map.of("search1", qr);

		ZuliaTestRunner tests = createRunner(List.of(createTest("same", "true"), createTest("same", "false")));
		Assertions.assertTrue(Assertions.assertThrows(IllegalArgumentException.class, () -> tests.evaluate(Instant.now(), results)).getMessage()
				.contains("Test name <same> is used more than once"));

		ZuliaTestRunner values = createRunner(List.of(), List.of(createValue("v", "1"), createValue("v", "2")));
		Assertions.assertTrue(Assertions.assertThrows(IllegalArgumentException.class, () -> values.evaluate(Instant.now(), results)).getMessage()
				.contains("Value name <v> is used more than once"));
	}

	@Test
	public void reservedWordsAreRejectedAsNames() {
		QueryResultObject qr = new QueryResultObject();
		qr.count = 1;
		Map<String, QueryResultObject> results = Map.of("search1", qr);

		ZuliaTestRunner variable = createRunner(List.of());
		variable.zuliaTestConfig.setVariables(Map.of("this", 5));
		Assertions.assertTrue(Assertions.assertThrows(IllegalArgumentException.class, () -> variable.evaluate(Instant.now(), results)).getMessage()
				.contains("reserved JavaScript word"));

		ZuliaTestRunner function = withFunctions(createRunner(List.of()), createFunction("class", List.of(), "1", null));
		Assertions.assertTrue(Assertions.assertThrows(IllegalArgumentException.class, () -> function.evaluate(Instant.now(), results)).getMessage()
				.contains("reserved JavaScript word"));

		ZuliaTestRunner argument = withFunctions(createRunner(List.of()), createFunction("f", List.of("new"), "1", null));
		Assertions.assertTrue(Assertions.assertThrows(IllegalArgumentException.class, () -> argument.evaluate(Instant.now(), results)).getMessage()
				.contains("argument <new> is a reserved"));
	}

	@Test
	public void savedAsAFieldOfSomethingElseIsNotASavedReference() {
		ZuliaTestRunner runner = createRunner(List.of(createTest("field", "search1.doc[0].saved.flag == 1")));
		Document doc = new Document();
		doc.put("saved", new Document("flag", 1));
		QueryResultObject qr = new QueryResultObject();
		qr.count = 1;
		qr.doc = new ArrayList<>();
		qr.doc.add(new DocumentProxyObject(doc));

		Assertions.assertTrue(runner.evaluate(Instant.now(), Map.of("search1", qr)).testResults().getFirst().isPassed());
	}
}
