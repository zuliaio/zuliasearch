package io.zulia.server.cmd;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import io.zulia.client.config.ZuliaPoolConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.log.LogUtil;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

public class ZuliaImport {

	private static final Logger LOG = Logger.getLogger(ZuliaImport.class.getSimpleName());

	public static class ZuliaImportArgs extends ZuliaBaseArgs {

		@Parameter(names = "--idField", description = "ID field name, defaults to 'id'")
		private String idField = "id";

		@Parameter(names = "--dir", description = "Full path to the zuliaexport directory.", required = true)
		private String dir;

	}

	public static void main(String[] args) {

		LogUtil.init();

		ZuliaImport.ZuliaImportArgs zuliaImportArgs = new ZuliaImportArgs();

		JCommander jCommander = JCommander.newBuilder().addObject(zuliaImportArgs).build();

		try {

			jCommander.parse(args);

			ZuliaPoolConfig zuliaPoolConfig = new ZuliaPoolConfig().addNode(zuliaImportArgs.address, zuliaImportArgs.port).setNodeUpdateEnabled(false);
			ZuliaWorkPool workPool = new ZuliaWorkPool(zuliaPoolConfig);

			String dir = zuliaImportArgs.dir;
			String index = zuliaImportArgs.index;
			String idField = zuliaImportArgs.idField;
			Integer threads = zuliaImportArgs.threads;
			Boolean skipExistingFiles = zuliaImportArgs.skipExistingFiles;

			if (index != null) {
				doImport(workPool, dir, index, idField, threads, skipExistingFiles);
			}

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

	private static void doImport(ZuliaWorkPool workPool, String dir, String index, String idField, Integer threads, Boolean skipExistingFiles)
			throws Exception {
		String inputDir = dir + File.separator + index;
		String recordsFilename = inputDir + File.separator + index + ".json";

		if (Files.exists(Paths.get(recordsFilename))) {
			AtomicInteger count = new AtomicInteger();
			LOG.info("Starting to index records for index <" + index + ">");
			ZuliaCmdUtil.index(inputDir, recordsFilename, idField, index, workPool, count, threads, skipExistingFiles);
			LOG.info("Finished indexing for index <" + index + "> with total records: " + count);
		}
		else {
			System.err.println("File <" + recordsFilename + "> does not exist in the given dir <" + dir + ">");
			System.exit(9);
		}
	}

}
