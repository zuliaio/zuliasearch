package io.zulia.testing;

import com.google.common.base.Supplier;
import io.zulia.client.command.builder.Search;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.SearchResult;
import io.zulia.testing.config.FunctionConfig;
import io.zulia.testing.config.SearchConfig;
import io.zulia.testing.config.TestConfig;
import io.zulia.testing.config.ValueConfig;
import io.zulia.testing.config.ZuliaTestConfig;
import io.zulia.testing.js.JsContext;
import io.zulia.testing.js.SavedUnavailableException;
import io.zulia.testing.js.dto.JsValues;
import io.zulia.testing.js.dto.QueryResultObject;
import io.zulia.testing.js.dto.SavedValuesProxyObject;
import io.zulia.testing.js.dto.UnavailableSavedProxyObject;
import io.zulia.testing.result.TestResult;
import io.zulia.testing.result.TestRun;
import io.zulia.testing.result.TestRunReport;
import io.zulia.testing.result.TestRunReport.Outcome;
import io.zulia.testing.result.TestStatus;
import io.zulia.testing.search.SearchExecutor;
import io.zulia.testing.search.SearchSteps;
import org.graalvm.polyglot.Value;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Runs a YAML test config: executes the searches, sets up a JavaScript context with the results, saved values, variables, script and
 * functions, then evaluates every test and records every value. A test or value whose expression cannot be evaluated is reported as ERROR and
 * the rest of the run continues.
 */
public class ZuliaTestRunner {
	private static final Logger LOG = LoggerFactory.getLogger(ZuliaTestRunner.class);

	/**
	 * Where {@code saved} comes from for this run.
	 */
	private sealed interface SavedSource {
		/**
		 * No report was loaded. A read of {@code saved} is an error for the expression that does it.
		 */
		record None() implements SavedSource {
		}

		/**
		 * First run of a config that compares against {@code saved}. A read skips the expression.
		 */
		record Baseline() implements SavedSource {
		}

		/**
		 * Values of an earlier run, and the names that run skipped and so never recorded.
		 */
		record Previous(Map<String, Object> values, Set<String> skippedNames) implements SavedSource {
		}
	}

	protected final ZuliaTestConfig zuliaTestConfig;
	private final SearchExecutor searchExecutor;

	private SavedSource savedSource = new SavedSource.None();
	private Map<String, Object> variableOverrides = Map.of();
	private LocalDate asOf = LocalDate.now();
	private RunSetup activeSetup;

	public ZuliaTestRunner(ZuliaTestConfig zuliaTestConfig) {
		this.zuliaTestConfig = zuliaTestConfig;
		this.searchExecutor = new SearchExecutor(zuliaTestConfig, buildConnectionSupplier());
	}

	/**
	 * Values recorded by a previous run, exposed to expressions as {@code saved.name}.
	 */
	public ZuliaTestRunner setSavedValues(Map<String, Object> savedValues) {
		this.savedSource = new SavedSource.Previous(savedValues, Set.of());
		return this;
	}

	/**
	 * The report of a previous run. Its values are exposed as {@code saved.name}. A value that run skipped is skipped again by whatever reads
	 * it here, so a value that itself compares against {@code saved} becomes available one run later instead of stopping the run.
	 */
	public ZuliaTestRunner setPreviousReport(TestRunReport previous) {
		List<Outcome> unrecorded = previous.unrecordedValues() != null ? previous.unrecordedValues() : List.of();
		Set<String> skippedNames = unrecorded.stream().filter(v -> v.status() == TestStatus.SKIPPED).map(Outcome::name).collect(Collectors.toSet());
		this.savedSource = new SavedSource.Previous(previous.values(), skippedNames);
		return this;
	}

	/**
	 * Marks this as the first run of a config that compares against {@code saved}. Tests and values that read {@code saved} are reported as
	 * SKIPPED and everything else runs, so the run can produce the report the next one loads.
	 */
	public ZuliaTestRunner recordBaseline() {
		this.savedSource = new SavedSource.Baseline();
		return this;
	}

	/**
	 * The date {@code now} and the named date aliases resolve from. Defaults to today and is recorded in the report.
	 */
	public ZuliaTestRunner setAsOf(LocalDate asOf) {
		this.asOf = asOf;
		return this;
	}

	/**
	 * Variables that replace or add to the config's {@code variables}, typically from the command line.
	 */
	public ZuliaTestRunner setVariableOverrides(Map<String, Object> variableOverrides) {
		this.variableOverrides = variableOverrides;
		return this;
	}

	public LocalDate getAsOf() {
		return asOf;
	}

	public TestRun run() throws Exception {
		Instant started = Instant.now();
		return withSetup(() -> evaluate(started, buildAndRunQueries()));
	}

	public List<TestResult> runTests() throws Exception {
		return withSetup(() -> evaluateTestsWithQueryResults(buildAndRunQueries()));
	}

	@NotNull
	protected Map<String, Supplier<ZuliaWorkPool>> buildConnectionSupplier() {
		return SearchExecutor.buildConnectionSupplier(zuliaTestConfig);
	}

	@NotNull
	protected Map<String, QueryResultObject> buildAndRunQueries() throws Exception {
		return searchExecutor.runSearches(new HookedSearchSteps());
	}

	/**
	 * Forwards to the protected methods of this runner, so an override in a subclass is what the search loop calls.
	 */
	private final class HookedSearchSteps implements SearchSteps {

		@Override
		public Search buildSearch(String indexName, SearchConfig searchConfig) {
			return ZuliaTestRunner.this.buildSearch(indexName, searchConfig);
		}

		@Override
		public QueryResultObject buildQueryResultObject(SearchResult searchResult, SearchConfig searchConfig) {
			return ZuliaTestRunner.this.buildQueryResultObject(searchResult, searchConfig);
		}
	}

	@NotNull
	protected Search buildSearch(String indexName, SearchConfig searchConfig) {
		return searchExecutor.buildSearch(indexName, searchConfig, setup().templates());
	}

	@NotNull
	protected QueryResultObject buildQueryResultObject(SearchResult searchResult, SearchConfig queryConfig) {
		return searchExecutor.buildQueryResultObject(searchResult, queryConfig);
	}

	@NotNull
	protected List<TestResult> evaluateTestsWithQueryResults(Map<String, QueryResultObject> resultMap) {
		return evaluate(Instant.now(), resultMap).testResults();
	}

	@NotNull
	protected TestRun evaluate(Instant started, Map<String, QueryResultObject> resultMap) {
		RunSetup setup = setup();
		try (JsContext js = new JsContext()) {
			bindGlobals(js, setup, resultMap);
			List<TestResult> testResults = runTests(js, setup.templates());
			RecordedValues recorded = recordValues(js, setup.templates());
			return new TestRun(started, asOf, testResults, recorded.values(), recorded.unrecorded());
		}
	}

	/**
	 * The resolved variables and the resolver built from them.
	 */
	private record RunSetup(Map<String, Object> variables, TemplateResolver templates) {
	}

	private interface RunStep<T> {
		T run() throws Exception;
	}

	/**
	 * Checks the config and resolves the variables once, before any search, and shares the result with every hook called during the run.
	 */
	private <T> T withSetup(RunStep<T> step) throws Exception {
		activeSetup = prepare();
		try {
			return step.run();
		}
		finally {
			activeSetup = null;
		}
	}

	/**
	 * The setup of the run in progress, or a fresh one when a hook is called on its own.
	 */
	private RunSetup setup() {
		return activeSetup != null ? activeSetup : prepare();
	}

	/**
	 * Everything that can be checked without a JavaScript context is rejected here, before a search runs: duplicate names, a name used by two
	 * kinds of thing, invalid functions and invalid variables. A clash with a JavaScript built-in and a script syntax error surface later,
	 * when the globals are bound.
	 */
	private RunSetup prepare() {
		requireUniqueNames("Test", zuliaTestConfig.getTests(), TestConfig::getName);
		requireUniqueNames("Value", zuliaTestConfig.getValues(), ValueConfig::getName);
		requireUniqueNames("Function", zuliaTestConfig.getFunctions(), FunctionConfig::getName);
		if (zuliaTestConfig.getFunctions() != null) {
			zuliaTestConfig.getFunctions().forEach(JsContext::validateFunction);
		}
		Map<String, Object> variables = TestVariables.resolve(zuliaTestConfig, variableOverrides, asOf);
		requireDistinctGlobals(variables);
		return new RunSetup(variables, new TemplateResolver(asOf, variables));
	}

	private static <T> void requireUniqueNames(String kind, List<T> configs, Function<T, String> name) {
		if (configs == null) {
			return;
		}
		Set<String> seen = new HashSet<>();
		for (T config : configs) {
			if (!seen.add(name.apply(config))) {
				throw new IllegalArgumentException(kind + " name <" + name.apply(config) + "> is used more than once");
			}
		}
	}

	/**
	 * Searches, variables and functions share the JavaScript global scope, so one name cannot belong to two of them.
	 */
	private void requireDistinctGlobals(Map<String, Object> variables) {
		Map<String, String> kindByName = new HashMap<>();
		if (zuliaTestConfig.getSearches() != null) {
			zuliaTestConfig.getSearches().forEach(search -> kindByName.put(search.getName(), "search"));
		}
		for (String variable : variables.keySet()) {
			claimGlobal(kindByName, variable, "variable");
		}
		if (zuliaTestConfig.getFunctions() != null) {
			zuliaTestConfig.getFunctions().forEach(function -> claimGlobal(kindByName, function.getName(), "function"));
		}
	}

	private static void claimGlobal(Map<String, String> kindByName, String name, String kind) {
		String existing = kindByName.putIfAbsent(name, kind);
		if (existing != null) {
			throw new IllegalArgumentException("Name <" + name + "> is used by a " + existing + " and a " + kind + ". They share one global scope, rename one");
		}
	}

	/**
	 * Order matters: results first, then saved, date(), variables, the script, and finally functions, so each layer can use the ones before it.
	 */
	private void bindGlobals(JsContext js, RunSetup setup, Map<String, QueryResultObject> resultMap) {
		TemplateResolver templates = setup.templates();
		for (Map.Entry<String, QueryResultObject> result : resultMap.entrySet()) {
			js.bindNew("Search", result.getKey(), result.getValue());
			if (zuliaTestConfig.isLogSearchResults()) {
				LOG.info("Search result {}:\n{}", result.getKey(), js.toJson(result.getKey()));
			}
		}
		bindSaved(js);
		js.bindDateFunction(templates);
		for (Map.Entry<String, Object> variable : setup.variables().entrySet()) {
			js.bindNew("Variable", variable.getKey(), JsValues.toJs(variable.getValue()));
		}
		if (zuliaTestConfig.getScript() != null) {
			js.runScript("inline", zuliaTestConfig.getScript());
		}
		if (zuliaTestConfig.getFunctions() != null) {
			for (FunctionConfig function : zuliaTestConfig.getFunctions()) {
				js.defineFunction(function, templates);
			}
		}
	}

	private void bindSaved(JsContext js) {
		switch (savedSource) {
			case SavedSource.Previous previous ->
					js.bindNew("Saved values", JsContext.SAVED, new SavedValuesProxyObject(previous.values(), previous.skippedNames()));
			case SavedSource.Baseline _ -> js.bindNew("Saved values", JsContext.SAVED, new UnavailableSavedProxyObject(
					name -> new SavedUnavailableException("Uses " + JsContext.SAVED + "." + name + ", which is not available while recording a baseline")));
			case SavedSource.None _ -> {
				// A config written before saved existed may already have a search with this name. Leave it alone.
				if (!js.isDefined(JsContext.SAVED)) {
					js.bindNew("Saved values", JsContext.SAVED, new UnavailableSavedProxyObject(name -> new IllegalArgumentException(
							"Reads " + JsContext.SAVED + "." + name + " but no saved values were loaded. Pass a previous run report (--previousReport), "
									+ "or record a first report without one (--recordBaseline)")));
				}
			}
		}
	}

	private List<TestConfig> tests() {
		return zuliaTestConfig.getTests() != null ? zuliaTestConfig.getTests() : List.of();
	}

	private List<TestResult> runTests(JsContext js, TemplateResolver templates) {
		List<TestResult> testResults = new ArrayList<>();
		for (TestConfig testConfig : tests()) {
			LOG.info("Running Test {}", testConfig.getName());
			TestResult testResult = new TestResult();
			testResult.setTestId(testConfig.getName());
			testResult.setTestConfig(testConfig);
			try {
				testResult.setPassed(evaluateTest(js, templates, testConfig));
				LOG.info("Test {} {}", testConfig.getName(), testResult.isPassed() ? "Passed" : "Failed");
			}
			catch (SavedUnavailableException e) {
				testResult.setStatus(TestStatus.SKIPPED);
				testResult.setMessage(e.getMessage());
				LOG.info("Test {} Skipped: {}", testConfig.getName(), e.getMessage());
			}
			catch (RuntimeException e) {
				testResult.setStatus(TestStatus.ERROR);
				testResult.setMessage(reason(e));
				LOG.error("Test {} Error: {}", testConfig.getName(), reason(e));
			}
			testResults.add(testResult);
		}
		return testResults;
	}

	private static String reason(RuntimeException e) {
		return e.getMessage() != null ? e.getMessage() : e.toString();
	}

	private static boolean evaluateTest(JsContext js, TemplateResolver templates, TestConfig testConfig) {
		Value result = js.evaluate("test", testConfig.getName(), templates.resolve(testConfig.getExpr()));
		if (!result.isBoolean()) {
			throw new IllegalArgumentException(
					"Test <" + testConfig.getName() + "> expression <" + testConfig.getExpr() + "> must evaluate to a boolean but returned <" + result + ">");
		}
		return result.asBoolean();
	}

	/**
	 * The values that were recorded, and an outcome for each one that was not.
	 */
	private record RecordedValues(Map<String, Object> values, List<Outcome> unrecorded) {
	}

	private RecordedValues recordValues(JsContext js, TemplateResolver templates) {
		Map<String, Object> values = new LinkedHashMap<>();
		List<Outcome> unrecorded = new ArrayList<>();
		if (zuliaTestConfig.getValues() != null) {
			for (ValueConfig valueConfig : zuliaTestConfig.getValues()) {
				String name = valueConfig.getName();
				try {
					Object value = js.toJava(name, js.evaluate("value", name, templates.resolve(valueConfig.getExpr())));
					LOG.info("Value {} = {}", name, value);
					values.put(name, value);
				}
				catch (SavedUnavailableException e) {
					unrecorded.add(new Outcome(name, TestStatus.SKIPPED, e.getMessage()));
					LOG.info("Value {} Skipped: {}", name, e.getMessage());
				}
				catch (RuntimeException e) {
					unrecorded.add(new Outcome(name, TestStatus.ERROR, reason(e)));
					LOG.error("Value {} Error: {}", name, reason(e));
				}
			}
		}
		return new RecordedValues(values, unrecorded);
	}
}
