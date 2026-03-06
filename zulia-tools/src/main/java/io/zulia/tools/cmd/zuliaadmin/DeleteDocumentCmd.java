package io.zulia.tools.cmd.zuliaadmin;

import io.zulia.client.command.DeleteDocument;
import io.zulia.client.command.DeleteFull;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.tools.cmd.ZuliaAdmin;
import io.zulia.tools.cmd.common.SingleIndexArgs;
import picocli.CommandLine;

import java.util.concurrent.Callable;

@CommandLine.Command(name = "deleteDocument", aliases = "deleteDoc", description = "Deletes a document by unique id from an index")
public class DeleteDocumentCmd implements Callable<Integer> {

	@CommandLine.ParentCommand
	private ZuliaAdmin zuliaAdmin;

	@CommandLine.Mixin
	private SingleIndexArgs singleIndexArgs;

	@CommandLine.Option(names = "--id", description = "Unique id of the document to delete", required = true)
	private String id;

	@CommandLine.Option(names = "--deleteAssociated", description = "Also delete all associated files")
	private boolean deleteAssociated;

	@Override
	public Integer call() throws Exception {

		ZuliaWorkPool zuliaWorkPool = zuliaAdmin.getConnection();

		String index = singleIndexArgs.getIndex();

		if (deleteAssociated) {
			zuliaWorkPool.delete(new DeleteFull(id, index));
		}
		else {
			zuliaWorkPool.delete(new DeleteDocument(id, index));
		}

		System.out.println("Deleted document with id <" + id + "> from index <" + index + ">" + (deleteAssociated ? " (including associated files)" : ""));

		return CommandLine.ExitCode.OK;
	}
}
