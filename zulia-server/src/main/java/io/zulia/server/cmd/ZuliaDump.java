package io.zulia.server.cmd;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Charsets;
import com.google.protobuf.util.JsonFormat;
import io.zulia.client.command.FetchLargeAssociated;
import io.zulia.client.command.GetIndexConfig;
import io.zulia.client.config.ZuliaPoolConfig;
import io.zulia.client.pool.WorkPool;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.GetIndexesResult;
import io.zulia.log.LogUtil;

import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ZuliaDump {

	private static final Logger LOG = Logger.getLogger(ZuliaDump.class.getSimpleName());

	public static class ZuliaDumpArgs extends ZuliaBaseArgs {

		@Parameter(names = "--indexes", description = "Comma separated or name* for wild card multiple index names.")
		private String indexes;

		@Parameter(names = "--out", description = "Full path to the output directory. [Defaults to current_directory/zuliadump")
		private String out = System.getProperty("user.dir");

		@Parameter(names = "--q", description = "Zulia query, matches all docs by default.")
		private String q = "*:*";

		@Parameter(names = "--rows", description = "Number of records to return. [Defaults to 1000]")
		private Integer rows = 1000;

		@Parameter(names = "--includeAssociatedDocs", description = "Include Associated Documents in the dump.")
		private boolean includeAssociatedDocs = false;

		@Parameter(names = "--sortById", description = "Sort results by Id.")
		private boolean sortById = false;

		@Parameter(names = "--idField", description = "ID Field Name. [Defaults to id]")
		private String idField = "id";
	}

	public static void main(String[] args) {

		LogUtil.init();

		ZuliaDumpArgs zuliaDumpArgs = new ZuliaDumpArgs();

		JCommander jCommander = JCommander.newBuilder().addObject(zuliaDumpArgs).build();

		try {

			jCommander.parse(args);

			ZuliaPoolConfig zuliaPoolConfig = new ZuliaPoolConfig().addNode(zuliaDumpArgs.address, zuliaDumpArgs.port).setNodeUpdateEnabled(false);
			ZuliaWorkPool workPool = new ZuliaWorkPool(zuliaPoolConfig);

			String index = zuliaDumpArgs.index;
			String indexes = zuliaDumpArgs.indexes;
			boolean includeAssociatedDocs = zuliaDumpArgs.includeAssociatedDocs;

			if (index == null && indexes == null) {
				LOG.log(Level.SEVERE, "Please pass in an index name.");
				jCommander.usage();
				System.exit(2);
			}

			String q = zuliaDumpArgs.q;
			Integer rows = zuliaDumpArgs.rows;
			String out = zuliaDumpArgs.out;
			String idField = zuliaDumpArgs.idField;
			boolean sortById = zuliaDumpArgs.sortById;

			Set<String> uniqueIds = new HashSet<>();

			if (indexes != null) {

				if (indexes.contains(",")) {
					for (String ind : indexes.split(",")) {
						queryAndWriteOutput(workPool, ind, q, rows, out, idField, uniqueIds, sortById);
						if (includeAssociatedDocs) {
							fetchAssociatedDocs(workPool, ind, out, uniqueIds);
						}
					}
				}
				else if (indexes.contains("*")) {
					GetIndexesResult indexesResult = workPool.getIndexes();
					for (String ind : indexesResult.getIndexNames()) {
						if (ind.startsWith(indexes.replace("*", ""))) {
							queryAndWriteOutput(workPool, ind, q, rows, out, idField, uniqueIds, sortById);
							if (includeAssociatedDocs) {
								fetchAssociatedDocs(workPool, ind, out, uniqueIds);
							}
						}
					}
				}
			}
			else {
				queryAndWriteOutput(workPool, index, q, rows, out, idField, uniqueIds, sortById);
				if (includeAssociatedDocs) {
					fetchAssociatedDocs(workPool, index, out, uniqueIds);
				}
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

	private static void queryAndWriteOutput(ZuliaWorkPool workPool, String index, String q, Integer rows, String outputDir, String idField,
			Set<String> uniqueIds, boolean sortById) throws Exception {

		// create zuliadump dir first
		String zuliaDumpDir = outputDir + File.separator + "zuliadump";
		if (!Files.exists(Paths.get(zuliaDumpDir))) {
			Files.createDirectory(Paths.get(zuliaDumpDir));
		}

		// create index dir
		String indOutputDir = zuliaDumpDir + File.separator + index;
		if (!Files.exists(Paths.get(indOutputDir))) {
			Files.createDirectory(Paths.get(indOutputDir));
		}

		String recordsFilename = indOutputDir + File.separator + index + ".json";
		String settingsFilename = indOutputDir + File.separator + index + "_settings.json";

		AtomicInteger count = new AtomicInteger();
		LOG.info("Dumping index <" + index + ">");
		ZuliaCmdUtil.writeOutput(recordsFilename, index, q, rows, workPool, count, idField, uniqueIds, sortById);
		LOG.info("Finished dumping index <" + index + ">, total: " + count);

		try (FileWriter fileWriter = new FileWriter(settingsFilename, Charsets.UTF_8)) {
			LOG.info("Writing settings for index <" + index + ">");
			JsonFormat.Printer printer = JsonFormat.printer();
			fileWriter.write(printer.print(workPool.getIndexConfig(new GetIndexConfig(index)).getIndexConfig().getIndexSettings()));
			LOG.info("Finished writing settings for index <" + index + ">");
		}

	}

	private static void fetchAssociatedDocs(ZuliaWorkPool workPool, String index, String outputDir, Set<String> uniqueIds) throws Exception {

		String zuliaDumpDir = outputDir + File.separator + "zuliadump";
		String indOutputDir = zuliaDumpDir + File.separator + index;

		LOG.info("Starting to dump associated docs for <" + uniqueIds.size() + "> documents.");
		AtomicInteger count = new AtomicInteger(0);
		WorkPool threadPool = new WorkPool(4);
		for (String uniqueId : uniqueIds) {
			threadPool.executeAsync(() -> {

				workPool.fetchLargeAssociated(
						new FetchLargeAssociated(uniqueId, index, Paths.get(indOutputDir + File.separator + uniqueId.replaceAll("/", "_") + ".zip").toFile()));
				if (count.incrementAndGet() % 1000 == 0) {
					LOG.info("Associated docs dumped so far: " + count);
				}

				return null;
			});
		}
		LOG.info("Finished dumping associated docs for <" + uniqueIds.size() + "> documents.");

		try {
			threadPool.shutdown();
		}
		catch (Throwable t) {
			LOG.log(Level.SEVERE, "Could not shut down the thread pool.", t);
			System.exit(9);
		}
	}

}
