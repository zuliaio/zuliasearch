package io.zulia.tools.cmd.zuliaadmin;

import io.zulia.client.command.StoreLargeAssociated;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.tools.cmd.ZuliaAdmin;
import io.zulia.tools.cmd.common.SingleIndexArgs;
import picocli.CommandLine;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "storeAssociatedFile", aliases = "storeFile", description = "Stores an associated file in Zulia from a file on disk")
public class StoreAssociatedFileCmd implements Callable<Integer> {

	@CommandLine.ParentCommand
	private ZuliaAdmin zuliaAdmin;

	@CommandLine.Mixin
	private SingleIndexArgs singleIndexArgs;

	@CommandLine.Option(names = "--id", description = "Record id to associate file with", required = true)
	private String id = "id";

	@CommandLine.Option(names = "--fileName", description = "File name in Zulia", required = true)
	private String fileName;

	@CommandLine.Option(names = "--fileToStore", description = "File to store", required = true)
	private File fileToStore;

	@Override
	public Integer call() throws Exception {

		ZuliaWorkPool zuliaWorkPool = zuliaAdmin.getConnection();

		String index = singleIndexArgs.getIndex();

		if (!fileToStore.exists()) {
			throw new FileNotFoundException(fileToStore.getAbsolutePath() + " does not exist");
		}

		StoreLargeAssociated storeLargeAssociated = new StoreLargeAssociated(id, index, fileName, Files.readAllBytes(fileToStore.toPath()));
		zuliaWorkPool.storeLargeAssociated(storeLargeAssociated);

		return CommandLine.ExitCode.OK;
	}
}
