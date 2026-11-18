package io.zulia.tools.cmd;

import io.zulia.cmd.common.ShowStackArgs;
import io.zulia.cmd.common.ZuliaCommonCmd;
import io.zulia.cmd.common.ZuliaVersionProvider;
import io.zulia.data.output.FileDataOutputStream;
import io.zulia.data.target.spreadsheet.csv.CSVTarget;
import io.zulia.testing.ZuliaTestRunner;
import io.zulia.testing.config.ZuliaTestConfig;
import io.zulia.testing.result.TestResult;
import io.zulia.testing.result.TestRun;
import io.zulia.testing.result.TestRunReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import picocli.CommandLine;

import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "zuliatest", versionProvider = ZuliaVersionProvider.class, scope = CommandLine.ScopeType.INHERIT)
public class ZuliaTest implements Callable<Integer> {

	private static final Logger LOG = LoggerFactory.getLogger(ZuliaTest.class);

	private static final int EXIT_TEST_FAILED = 1;
	private static final int EXIT_EVALUATION_ERROR = 2;

	@CommandLine.Option(names = "--testConfig", description = "Full path to the test config yaml file", required = true)
	private String testConfig;

	@CommandLine.Option(names = "--testOutput", description = "Full path to the test output csv file", required = true)
	private String testOutput;

	@CommandLine.Option(names = "--reportOutput", description = "Full path to write the run report json (timestamp, recorded values, test outcomes)")
	private String reportOutput;

	@CommandLine.Option(names = "--previousReport", description = "Full path to a previous run report json whose values are exposed to expressions as saved.<name>")
	private String previousReport;

	@CommandLine.Option(names = "--recordBaseline", description = "First run of a config that compares against saved values. Tests and values that use saved are marked SKIPPED so the run can write the report the next run loads. Cannot be combined with --previousReport")
	private boolean recordBaseline;

	@CommandLine.Option(names = "--asOf", description = "Date (yyyy-MM-dd) the run is anchored to, defaults to today")
	private LocalDate asOf;

	@CommandLine.Option(names = "--var", description = "Set or override a config variable, name=value. The value is read like a YAML scalar, so numbers, true/false and dates keep their type. Quote it to force text. Repeatable")
	private Map<String, String> variables = new LinkedHashMap<>();

	@CommandLine.Spec
	private CommandLine.Model.CommandSpec spec;

	@CommandLine.Mixin
	private ShowStackArgs showStackArgs;

	@Override
	public Integer call() throws Exception {

		Path testConfigPath = Paths.get(testConfig);
		if (Files.exists(testConfigPath)) {
			Yaml yaml = new Yaml();
			try (FileInputStream fileInputStream = new FileInputStream(testConfigPath.toFile())) {
				ZuliaTestConfig zuliaTestConfig = yaml.loadAs(fileInputStream, ZuliaTestConfig.class);
				LOG.info("Running tests from {}", testConfig);
				ZuliaTestRunner zuliaTestRunner = new ZuliaTestRunner(zuliaTestConfig);
				if (recordBaseline && previousReport != null) {
					throw new CommandLine.ParameterException(spec.commandLine(),
							"--recordBaseline was given together with --previousReport <" + previousReport + ">. Use --recordBaseline for the first run only");
				}
				if (recordBaseline) {
					zuliaTestRunner.recordBaseline();
				}
				if (previousReport != null) {
					Path previousReportPath = Paths.get(previousReport);
					if (!Files.exists(previousReportPath)) {
						System.err.println("Previous report file " + previousReport + " does not exist or is not readable");
						System.exit(9);
					}
					TestRunReport previous = TestRunReport.read(previousReportPath);
					LOG.info("Loaded previous report {} (run at {}), saved values: {}", previousReport, previous.timestamp(), previous.values().keySet());
					zuliaTestRunner.setPreviousReport(previous);
				}
				if (asOf != null) {
					zuliaTestRunner.setAsOf(asOf);
				}
				if (!variables.isEmpty()) {
					Map<String, Object> overrides = new LinkedHashMap<>();
					variables.forEach((name, value) -> overrides.put(name, scalarVariable(yaml, name, value)));
					LOG.info("Variable overrides: {}", overrides);
					zuliaTestRunner.setVariableOverrides(overrides);
				}

				TestRun testRun = zuliaTestRunner.run();

				LOG.info("Writing results to {}", testOutput);
				FileDataOutputStream dataOutputStream = FileDataOutputStream.from(testOutput, true);
				try (CSVTarget csvDataTarget = CSVTarget.withDefaults(dataOutputStream)) {
					for (TestResult testResult : testRun.testResults()) {
						csvDataTarget.writeRow(testResult.getTestId(), testResult.getStatus().name());
					}
				}

				if (reportOutput != null) {
					LOG.info("Writing run report to {}", reportOutput);
					testRun.toReport().write(Paths.get(reportOutput));
				}

				// 2 means the config could not be fully evaluated, 1 means it ran cleanly and a test failed
				if (testRun.anyErrors()) {
					System.exit(EXIT_EVALUATION_ERROR);
				}
				if (testRun.anyFailed()) {
					System.exit(EXIT_TEST_FAILED);
				}
			}

		}
		else {
			System.err.println("Yaml configuration file " + testConfig + " does not exist or is not readable");
			System.exit(9);
		}

		return CommandLine.ExitCode.OK;
	}

	private Object scalarVariable(Yaml yaml, String name, String value) {
		Object parsed = yaml.load(value);
		String structure = switch (parsed) {
			case Map<?, ?> _ -> "map";
			case List<?> _ -> "list";
			case null, default -> null;
		};
		if (structure != null) {
			throw new CommandLine.ParameterException(spec.commandLine(),
					"--var " + name + " must be a scalar but <" + value + "> parsed as a YAML " + structure + ". Quote the value to pass it as text");
		}
		return parsed;
	}

	public static void main(String[] args) {
		ZuliaCommonCmd.runCommandLine(new ZuliaTest(), args);
	}

}
