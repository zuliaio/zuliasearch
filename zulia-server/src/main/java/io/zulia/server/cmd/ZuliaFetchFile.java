package io.zulia.server.cmd;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import io.zulia.client.command.FetchLargeAssociated;
import io.zulia.client.config.ZuliaPoolConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.log.LogUtil;

import java.io.File;
import java.util.logging.Logger;

public class ZuliaFetchFile {

	private static final Logger ZuliaFetchFile = Logger.getLogger(ZuliaDump.class.getSimpleName());

	public static class ZuliaFetchFileArgs extends ZuliaBaseArgs {

		@Parameter(names = "--id", description = "Record id to fetch file from", required = true)
		private String id = "id";

		@Parameter(names = "--fileName", description = "File name in Zulia", required = true)
		private String fileName;

		@Parameter(names = "--outputFile", description = "Output file to save", required = true)
		private String outputFile;

	}

	public static void main(String[] args) {

		LogUtil.init();

		ZuliaFetchFileArgs zuliaFetchFileArgs = new ZuliaFetchFileArgs();

		JCommander jCommander = JCommander.newBuilder().addObject(zuliaFetchFileArgs).build();

		try {

			jCommander.parse(args);

			ZuliaPoolConfig zuliaPoolConfig = new ZuliaPoolConfig().addNode(zuliaFetchFileArgs.address, zuliaFetchFileArgs.port).setNodeUpdateEnabled(false);
			ZuliaWorkPool workPool = new ZuliaWorkPool(zuliaPoolConfig);

			//String uniqueId, String indexName, String fileName, File outputFile
			File outputFile = new File(zuliaFetchFileArgs.outputFile);

			FetchLargeAssociated storeLargeAssociated = new FetchLargeAssociated(zuliaFetchFileArgs.id, zuliaFetchFileArgs.index, zuliaFetchFileArgs.fileName,
					outputFile);
			workPool.fetchLargeAssociated(storeLargeAssociated);

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

}
