package io.zulia.server.cmd.zuliaadmin;

import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.message.ZuliaIndex;
import io.zulia.server.cmd.ZuliaAdmin;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(name = "createAlias", description = "Creates or updates an alias")
public class CreateAliasCmd implements Callable<Integer> {

	@CommandLine.ParentCommand
	private ZuliaAdmin zuliaAdmin;

	@Override
	public Integer call() throws Exception {

		ZuliaWorkPool zuliaWorkPool = zuliaAdmin.getConnection();

		System.out.printf("%40s | %40s\n", "Alias", "Index");
		for (ZuliaIndex.IndexAlias indexAlias : zuliaWorkPool.getNodes().getIndexAliases()) {
			System.out.printf("%40s | %40s\n", indexAlias.getAliasName(), indexAlias.getIndexName());
		}

		return CommandLine.ExitCode.OK;
	}
}
