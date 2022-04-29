package io.zulia.server.cmd.zuliaadmin;

import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.message.ZuliaIndex;
import io.zulia.server.cmd.ZuliaAdmin;
import io.zulia.server.cmd.ZuliaCommonCmd;
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

		ZuliaCommonCmd.printMagenta(String.format("%40s | %40s", "Alias", "Index"));

		List<ZuliaIndex.IndexAlias> indexAliases = zuliaWorkPool.getNodes().getIndexAliases();

		for (ZuliaIndex.IndexAlias indexAlias : indexAliases) {
			System.out.printf("%40s | %40s", indexAlias.getAliasName(), indexAlias.getIndexName());
		}

		return CommandLine.ExitCode.OK;
	}
}
