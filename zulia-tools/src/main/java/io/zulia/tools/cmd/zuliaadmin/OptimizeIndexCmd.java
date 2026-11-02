package io.zulia.tools.cmd.zuliaadmin;

import io.zulia.client.command.OptimizeIndex;
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

	@CommandLine.Option(names = "--maxSegments", description = "Merge each shard down to at most this many segments. Default merges each shard to one segment")
	private int maxSegments = 0;

	@Override
	public Integer call() throws Exception {

		ZuliaWorkPool zuliaWorkPool = zuliaAdmin.getConnection();

		Set<String> indexes = multipleIndexArgs.resolveIndexes(zuliaWorkPool);

		for (String index : indexes) {
			zuliaWorkPool.optimizeIndex(new OptimizeIndex(index).setMaxNumberOfSegments(maxSegments));
		}
		return CommandLine.ExitCode.OK;
	}
}
