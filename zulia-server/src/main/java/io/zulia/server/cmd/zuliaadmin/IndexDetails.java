package io.zulia.server.cmd.zuliaadmin;

import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.GetNumberOfDocsResult;
import io.zulia.server.cmd.ZuliaAdmin;
import io.zulia.server.cmd.common.MultipleIndexArgs;
import picocli.CommandLine;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "indexDetails", description = "Display details of index(es) specified by --index")
public class IndexDetails implements Callable<Integer> {

	public enum Sort {
		size,
		abc,
		given;
	}

	@CommandLine.ParentCommand
	private ZuliaAdmin zuliaAdmin;

	@CommandLine.Mixin
	private MultipleIndexArgs multipleIndexArgs;

	@CommandLine.Option(names = {"-s", "--sort"}, description = "Sort results by (${COMPLETION-CANDIDATES}).  default: ${DEFAULT-VALUE}")
	private Sort sort = Sort.given;

	@Override
	public Integer call() throws Exception {

		ZuliaWorkPool zuliaWorkPool = zuliaAdmin.getConnection();

		Set<String> indexes = multipleIndexArgs.resolveIndexes(zuliaWorkPool);

		if (indexes.size() > 1) {
			long total = 0;
			System.out.printf("%40s | %22s\n", "Index", "Count");

			Map<String, Long> indexToCount = new LinkedHashMap<>();

			for (String index : indexes) {
				GetNumberOfDocsResult numberOfDocs = zuliaWorkPool.getNumberOfDocs(index);
				indexToCount.put(index, numberOfDocs.getNumberOfDocs());
				total += numberOfDocs.getNumberOfDocs();
			}

			ArrayList<Map.Entry<String, Long>> entries = new ArrayList<>(indexToCount.entrySet());

			if (Sort.abc.equals(sort)) {
				entries.sort(Map.Entry.comparingByKey());
			}
			else if (Sort.size.equals(sort)) {
				entries.sort(Map.Entry.<String, Long>comparingByValue().reversed());
			}

			for (var entry : entries) {
				System.out.printf("%40s | %22s\n", entry.getKey(), entry.getValue());
			}


			System.out.printf("%40s | %22s\n", "", total);
		}
		else {
			GetNumberOfDocsResult numberOfDocs = zuliaWorkPool.getNumberOfDocs(indexes.iterator().next());
			System.out.println(numberOfDocs.getNumberOfDocs());
		}



		return CommandLine.ExitCode.OK;
	}
}
