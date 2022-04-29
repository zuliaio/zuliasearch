package io.zulia.server.cmd.zuliaadmin;

import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.message.ZuliaIndex;
import io.zulia.server.cmd.ZuliaAdmin;
import io.zulia.server.cmd.common.AliasArgs;
import picocli.CommandLine;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "displayAliases", description = "Displays aliases")
public class DisplayAliasesCmd implements Callable<Integer> {

	@CommandLine.ParentCommand
	private ZuliaAdmin zuliaAdmin;

	@CommandLine.Mixin
	private AliasArgs aliasArgs;

	@Override
	public Integer call() throws Exception {

		ZuliaWorkPool zuliaWorkPool = zuliaAdmin.getConnection();

		System.out.printf("%40s | %40s", "Alias", "Index");

		List<ZuliaIndex.IndexAlias> indexAliases = zuliaWorkPool.getNodes().getIndexAliases();

		Set<String> aliases = aliasArgs.resolveAliases(zuliaWorkPool);

		for (ZuliaIndex.IndexAlias indexAlias : indexAliases) {
			if (aliases.contains(indexAlias.getAliasName())) {
				System.out.printf("%40s | %40s", indexAlias.getAliasName(), indexAlias.getIndexName());
			}
		}

		return CommandLine.ExitCode.OK;
	}
}
