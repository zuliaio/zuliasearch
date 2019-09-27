package io.zulia.server.cmd;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import io.zulia.client.config.ZuliaPoolConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.GetIndexesResult;
import io.zulia.log.LogUtil;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ZuliaExport {

	private static final Logger LOG = Logger.getLogger(ZuliaDump.class.getSimpleName());

	public static class ZuliaExportArgs extends ZuliaBaseArgs {

		@Parameter(names = "--indexes", description = "Comma separated or name* for wild card multiple index names.")
		private String indexes;

		@Parameter(names = "--out", description = "Full path to the output directory. [Defaults to current_directory/zuliadump")
		private String out = System.getProperty("user.dir");

		@Parameter(names = "--q", description = "Zulia query, matches all docs by default.")
		private String q = "*:*";

		@Parameter(names = "--rows", description = "Number of records to return. [Defaults to 1000]")
		private Integer rows = 1000;
	}

	public static void main(String[] args) {

		LogUtil.init();

		ZuliaExportArgs zuliaExportArgs = new ZuliaExportArgs();

		JCommander jCommander = JCommander.newBuilder().addObject(zuliaExportArgs).build();

		try {

			jCommander.parse(args);

			ZuliaPoolConfig zuliaPoolConfig = new ZuliaPoolConfig().addNode(zuliaExportArgs.address, zuliaExportArgs.port).setNodeUpdateEnabled(false);
			ZuliaWorkPool workPool = new ZuliaWorkPool(zuliaPoolConfig);

			String index = zuliaExportArgs.index;
			String indexes = zuliaExportArgs.indexes;

			if (index == null && indexes == null) {
				LOG.log(Level.SEVERE, "Please pass in an index name.");
				jCommander.usage();
				System.exit(2);
			}

			String q = zuliaExportArgs.q;
			Integer rows = zuliaExportArgs.rows;
			String out = zuliaExportArgs.out;

			if (indexes != null) {

				if (indexes.contains(",")) {
					for (String ind : indexes.split(",")) {
						queryAndWriteOutput(workPool, ind, q, rows, out);
					}
				}
				else if (indexes.contains("*")) {
					GetIndexesResult indexesResult = workPool.getIndexes();
					for (String ind : indexesResult.getIndexNames()) {
						if (ind.startsWith(indexes.replace("*", ""))) {
							queryAndWriteOutput(workPool, ind, q, rows, out);
						}
					}
				}
			}
			else {
				queryAndWriteOutput(workPool, index, q, rows, out);
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

	private static void queryAndWriteOutput(ZuliaWorkPool workPool, String index, String q, Integer rows, String out) throws Exception {
		io.zulia.client.command.Query zuliaQuery;

		// create zuliaexport dir first
		String zuliaExportDir = out + File.separator + "zuliaexport";
		if (!Files.exists(Paths.get(zuliaExportDir))) {
			Files.createDirectory(Paths.get(zuliaExportDir));
		}

		// create index dir
		String indOutputDir = zuliaExportDir + File.separator + index;
		if (!Files.exists(Paths.get(indOutputDir))) {
			Files.createDirectory(Paths.get(indOutputDir));
		}

		String recordsFilename = indOutputDir + File.separator + index + ".json";

		AtomicInteger count = new AtomicInteger();
		LOG.info("Exporting from index <" + index + ">");
		ZuliaCmdUtil.writeOutput(recordsFilename, index, q, rows, workPool, count);
		LOG.info("Finished exporting from index <" + index + ">, total: " + count);

	}

}
