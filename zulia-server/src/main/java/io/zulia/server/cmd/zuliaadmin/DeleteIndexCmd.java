package io.zulia.server.cmd.zuliaadmin;

import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.server.cmd.ZuliaAdmin;
import io.zulia.server.cmd.common.MultipleIndexArgs;
import picocli.CommandLine;

import java.util.Set;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "deleteIndex", description = "Deletes index(es) specified by --index")
public class DeleteIndexCmd implements Callable<Integer> {

	@CommandLine.ParentCommand
	private ZuliaAdmin zuliaAdmin;

	@CommandLine.Mixin
	private MultipleIndexArgs multipleIndexArgs;

	@CommandLine.Option(names = { "-d", "--deleteAssociated" }, description = "Delete associated files")
	private boolean deleteAssociated;

	@Override
	public Integer call() throws Exception {

		ZuliaWorkPool zuliaWorkPool = zuliaAdmin.getConnection();

		Set<String> indexes = multipleIndexArgs.resolveIndexes(zuliaWorkPool);

		for (String index : indexes) {
			if (deleteAssociated) {
				zuliaWorkPool.deleteIndexAndAssociatedFiles(index);
			}
			else {
				zuliaWorkPool.deleteIndex(index);
			}
		}

		return CommandLine.ExitCode.OK;
	}
}
