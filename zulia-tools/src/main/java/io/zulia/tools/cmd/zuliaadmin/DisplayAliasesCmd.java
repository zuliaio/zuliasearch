package io.zulia.tools.cmd.zuliaadmin;

import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.cmd.common.ZuliaCommonCmd;
import io.zulia.message.ZuliaIndex;
import io.zulia.tools.cmd.ZuliaAdmin;
import io.zulia.util.IndexAliasUtil;
import picocli.CommandLine;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

@CommandLine.Command(name = "displayAliases", description = "Displays aliases")
public class DisplayAliasesCmd implements Callable<Integer> {

	@CommandLine.ParentCommand
	private ZuliaAdmin zuliaAdmin;

	@Override
	public Integer call() throws Exception {

		ZuliaWorkPool zuliaWorkPool = zuliaAdmin.getConnection();

		List<ZuliaIndex.IndexAlias> indexAliases = zuliaWorkPool.getNodes().getIndexAliases();
		ZuliaCommonCmd.printMagenta(String.format("%40s | %40s | %20s", "Alias", "Indexes", "Write Index"));

		for (ZuliaIndex.IndexAlias indexAlias : indexAliases) {
			String writeIndex = indexAlias.getWriteIndex();
			String indexes = IndexAliasUtil.getIndexNames(indexAlias).stream()
					.map(name -> name.equals(writeIndex) ? name + "*" : name)
					.collect(Collectors.joining(", "));
			System.out.printf("%40s | %40s | %20s", indexAlias.getAliasName(), indexes, writeIndex.isEmpty() ? "-" : writeIndex);
			System.out.println();
		}

		return CommandLine.ExitCode.OK;
	}
}
