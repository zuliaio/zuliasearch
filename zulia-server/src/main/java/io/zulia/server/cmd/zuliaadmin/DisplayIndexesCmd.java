package io.zulia.server.cmd.zuliaadmin;

import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.GetIndexesResult;
import io.zulia.server.cmd.ZuliaAdmin;
import picocli.CommandLine;

import java.util.List;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "displayIndexes", description = "Display the indexes")
public class DisplayIndexesCmd implements Callable<Integer> {

	@CommandLine.ParentCommand
	private ZuliaAdmin zuliaAdmin;

	@Override
	public Integer call() throws Exception {
		ZuliaWorkPool zuliaWorkPool = zuliaAdmin.getConnection();

		GetIndexesResult indexes = zuliaWorkPool.getIndexes();

		List<String> indexNames = indexes.getIndexNames();

		for (String index : indexNames) {
			System.out.println(index);
		}

		return CommandLine.ExitCode.OK;
	}
}
