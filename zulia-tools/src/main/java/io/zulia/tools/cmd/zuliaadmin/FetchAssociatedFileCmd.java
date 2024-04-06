package io.zulia.tools.cmd.zuliaadmin;

import io.zulia.client.command.FetchLargeAssociated;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.tools.cmd.ZuliaAdmin;
import io.zulia.tools.cmd.common.SingleIndexArgs;
import picocli.CommandLine;

import java.io.File;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "fetchAssociatedFile", aliases = "fetchFile", description = "Fetches an associated file and stores to disk")
public class FetchAssociatedFileCmd implements Callable<Integer> {

	@CommandLine.ParentCommand
	private ZuliaAdmin zuliaAdmin;

	@CommandLine.Mixin
	private SingleIndexArgs singleIndexArgs;

	@CommandLine.Option(names = "--id", description = "Record id to fetch file from", required = true)
	private String id;

	@CommandLine.Option(names = "--fileName", description = "File name in Zulia", required = true)
	private String fileName;

	@CommandLine.Option(names = "--outputFile", description = "Output file to save", required = true)
	private File outputFile;

	@Override
	public Integer call() throws Exception {

		ZuliaWorkPool zuliaWorkPool = zuliaAdmin.getConnection();

		String index = singleIndexArgs.getIndex();

		FetchLargeAssociated storeLargeAssociated = new FetchLargeAssociated(id, index, fileName, outputFile);
		zuliaWorkPool.fetchLargeAssociated(storeLargeAssociated);

		return CommandLine.ExitCode.OK;
	}
}
