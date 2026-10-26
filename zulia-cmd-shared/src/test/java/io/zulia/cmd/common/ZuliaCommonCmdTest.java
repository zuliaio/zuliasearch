package io.zulia.cmd.common;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.util.List;
import java.util.concurrent.Callable;

public class ZuliaCommonCmdTest {

	@CommandLine.Command(name = "probe")
	static class ProbeCmd implements Callable<Integer> {

		@CommandLine.Option(names = { "-q", "--query" })
		String query;

		@CommandLine.Option(names = { "-i", "--index" }, split = ",")
		List<String> indexes;

		@Override
		public Integer call() {
			return 0;
		}
	}

	@Test
	public void quotedPhraseQueryKeepsQuotesTest() {
		// what the shell delivers for -q '"machine learning"'
		Assertions.assertEquals("\"machine learning\"", run("-q", "\"machine learning\"").query);
		Assertions.assertEquals("\"machine learning\"", run("--query=\"machine learning\"").query);
		Assertions.assertEquals("\"lung cancer\" AND \"machine learning\"", run("-q", "\"lung cancer\" AND \"machine learning\"").query);
	}

	@Test
	public void unquotedValuesUnchangedTest() {
		ProbeCmd probe = run("-q", "title:\"machine learning\" AND cancer", "-i", "first,second");
		Assertions.assertEquals("title:\"machine learning\" AND cancer", probe.query);
		Assertions.assertEquals(List.of("first", "second"), probe.indexes);
	}

	private static ProbeCmd run(String... args) {
		ProbeCmd probe = new ProbeCmd();
		ZuliaCommonCmd.runCommandLine(probe, args);
		return probe;
	}

}
