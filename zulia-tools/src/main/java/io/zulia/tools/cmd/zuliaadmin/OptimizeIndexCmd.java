package io.zulia.tools.cmd.zuliaadmin;

import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.tools.cmd.ZuliaAdmin;
import io.zulia.tools.cmd.common.MultipleIndexArgs;
import picocli.CommandLine;

import java.util.Set;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "optimizeIndex", description = "Optimizes index(es) specified by --index")
public class OptimizeIndexCmd implements Callable<Integer> {

	@CommandLine.ParentCommand
	private ZuliaAdmin zuliaAdmin;

	@CommandLine.Mixin
	private MultipleIndexArgs multipleIndexArgs;

	@Override
	public Integer call() throws Exception {

		ZuliaWorkPool zuliaWorkPool = zuliaAdmin.getConnection();

		Set<String> indexes = multipleIndexArgs.resolveIndexes(zuliaWorkPool);

		for (String index : indexes) {
			zuliaWorkPool.optimizeIndex(index);
		}
		return CommandLine.ExitCode.OK;
	}
}
