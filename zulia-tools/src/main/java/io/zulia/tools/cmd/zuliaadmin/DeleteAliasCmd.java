package io.zulia.tools.cmd.zuliaadmin;

import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.tools.cmd.ZuliaAdmin;
import io.zulia.tools.cmd.common.AliasArgs;
import picocli.CommandLine;

import java.util.Set;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "deleteAlias", description = "Deletes aliases(es) specified by --alias")
public class DeleteAliasCmd implements Callable<Integer> {

	@CommandLine.ParentCommand
	private ZuliaAdmin zuliaAdmin;

	@CommandLine.Mixin
	private AliasArgs aliasArgs;

	@Override
	public Integer call() throws Exception {

		ZuliaWorkPool zuliaWorkPool = zuliaAdmin.getConnection();

		Set<String> aliases = aliasArgs.resolveAliases(zuliaWorkPool);

		for (String alias : aliases) {
			zuliaWorkPool.deleteIndexAlias(alias);
		}

		return CommandLine.ExitCode.OK;
	}
}
