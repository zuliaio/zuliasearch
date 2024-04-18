package io.zulia.tools.cmd.zuliaadmin;

import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.cmd.common.ZuliaCommonCmd;
import io.zulia.message.ZuliaIndex;
import io.zulia.tools.cmd.ZuliaAdmin;
import picocli.CommandLine;

import java.util.List;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "displayAliases", description = "Displays aliases")
public class DisplayAliasesCmd implements Callable<Integer> {

	@CommandLine.ParentCommand
	private ZuliaAdmin zuliaAdmin;

	@Override
	public Integer call() throws Exception {

		ZuliaWorkPool zuliaWorkPool = zuliaAdmin.getConnection();

		List<ZuliaIndex.IndexAlias> indexAliases = zuliaWorkPool.getNodes().getIndexAliases();
		ZuliaCommonCmd.printMagenta(String.format("%40s | %40s", "Alias", "Index"));

		for (ZuliaIndex.IndexAlias indexAlias : indexAliases) {
			System.out.printf("%40s | %40s", indexAlias.getAliasName(), indexAlias.getIndexName());
			System.out.println();
		}

		return CommandLine.ExitCode.OK;
	}
}
