package io.zulia.server.cmd;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import io.zulia.client.command.StoreLargeAssociated;
import io.zulia.client.config.ZuliaPoolConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.log.LogUtil;

import java.io.File;
import java.nio.file.Files;
import java.util.logging.Logger;

public class ZuliaStoreFile {

	private static final Logger LOG = Logger.getLogger(ZuliaStoreFile.class.getSimpleName());

	public static void main(String[] args) {

		LogUtil.init();

		ZuliaStoreFileArgs zuliaStoreFileArgs = new ZuliaStoreFileArgs();

		JCommander jCommander = JCommander.newBuilder().addObject(zuliaStoreFileArgs).build();

		try {

			jCommander.parse(args);

			ZuliaPoolConfig zuliaPoolConfig = new ZuliaPoolConfig().addNode(zuliaStoreFileArgs.address, zuliaStoreFileArgs.port).setNodeUpdateEnabled(false);
			ZuliaWorkPool workPool = new ZuliaWorkPool(zuliaPoolConfig);

			//String uniqueId, String indexName, String fileName, File fileToStore
			File fileToStore = new File(zuliaStoreFileArgs.fileToStore);

			if (!fileToStore.exists()) {
				System.err.println("File " + fileToStore.getAbsolutePath() + " does not exist");
				System.exit(3);
			}

			StoreLargeAssociated storeLargeAssociated = new StoreLargeAssociated(zuliaStoreFileArgs.id, zuliaStoreFileArgs.index, zuliaStoreFileArgs.fileName,
					Files.readAllBytes(fileToStore.toPath()));
			workPool.storeLargeAssociated(storeLargeAssociated);

		}
		catch (ParameterException e) {
			System.err.println(e.getMessage());
			jCommander.usage();
			System.exit(2);
		}
		catch (UnsupportedOperationException e) {
			System.err.println("Error: " + e.getMessage());
			System.exit(2);
		}
		catch (Exception e) {
			System.err.println("Error: " + e.getMessage());
			e.printStackTrace();
			System.exit(1);
		}

	}

	public static class ZuliaStoreFileArgs extends ZuliaBaseArgs {

		@Parameter(names = "--id", description = "Record id to associate file", required = true)
		private String id = "id";

		@Parameter(names = "--fileName", description = "File name in Zulia", required = true)
		private String fileName;

		@Parameter(names = "--fileToStore", description = "File to store in Zulia", required = true)
		private String fileToStore;

	}

}
