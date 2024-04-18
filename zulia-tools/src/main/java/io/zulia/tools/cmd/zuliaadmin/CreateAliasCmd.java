package io.zulia.tools.cmd.zuliaadmin;

import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.tools.cmd.ZuliaAdmin;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(name = "createAlias", description = "Creates or updates an alias")
public class CreateAliasCmd implements Callable<Integer> {

	@CommandLine.ParentCommand
	private ZuliaAdmin zuliaAdmin;

	//TODO: when an alias can point to multiple index we can use index info here
	@CommandLine.Option(names = { "-i", "--index" }, description = "Index that the alias points to", required = true)
	private String index;

	@CommandLine.Option(names = { "-a", "--alias" }, description = "Alias name", required = true)
	private String alias;

	@Override
	public Integer call() throws Exception {

		ZuliaWorkPool zuliaWorkPool = zuliaAdmin.getConnection();
		zuliaWorkPool.createIndexAlias(alias, index);
		return CommandLine.ExitCode.OK;
	}
}
