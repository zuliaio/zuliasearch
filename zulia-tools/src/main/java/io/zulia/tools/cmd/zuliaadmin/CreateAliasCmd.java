package io.zulia.tools.cmd.zuliaadmin;

import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.tools.cmd.ZuliaAdmin;
import picocli.CommandLine;

import java.util.List;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "createAlias", description = "Creates or updates an alias")
public class CreateAliasCmd implements Callable<Integer> {

	@CommandLine.ParentCommand
	private ZuliaAdmin zuliaAdmin;

	@CommandLine.Option(names = { "-i", "--index" }, description = "Index(es) that the alias points to", required = true, split = ",", arity = "1..*")
	private List<String> indexes;

	@CommandLine.Option(names = { "-a", "--alias" }, description = "Alias name", required = true)
	private String alias;

	@CommandLine.Option(names = { "-w", "--writeIndex" }, description = "Designated write index (must be one of the listed indexes; only meaningful when multiple indexes are given)")
	private String writeIndex;

	@Override
	public Integer call() throws Exception {

		ZuliaWorkPool zuliaWorkPool = zuliaAdmin.getConnection();
		if (indexes.size() == 1 && (writeIndex == null || writeIndex.isEmpty())) {
			zuliaWorkPool.createIndexAlias(alias, indexes.getFirst());
		}
		else {
			zuliaWorkPool.createIndexAlias(alias, indexes, writeIndex);
		}
		return CommandLine.ExitCode.OK;
	}
}
