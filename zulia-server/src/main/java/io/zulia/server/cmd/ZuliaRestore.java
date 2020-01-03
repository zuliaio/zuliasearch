package io.zulia.server.cmd;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Charsets;
import com.google.protobuf.util.JsonFormat;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.config.ZuliaPoolConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.log.LogUtil;
import io.zulia.message.ZuliaIndex;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ZuliaRestore {

	private static final Logger LOG = Logger.getLogger(ZuliaDump.class.getSimpleName());

	public static class ZuliaRestoreArgs extends ZuliaBaseArgs {

		@Parameter(names = "--idField", description = "ID field name, defaults to 'id'")
		private String idField = "id";

		@Parameter(names = "--dir", description = "Full path to the zuliadump directory.", required = true)
		private String dir;

		@Parameter(names = "--drop", description = "Drop the index before restoring.")
		private Boolean drop = false;

	}

	public static void main(String[] args) {

		LogUtil.init();

		ZuliaRestoreArgs zuliaRestoreArgs = new ZuliaRestoreArgs();

		JCommander jCommander = JCommander.newBuilder().addObject(zuliaRestoreArgs).build();

		try {

			jCommander.parse(args);

			ZuliaPoolConfig zuliaPoolConfig = new ZuliaPoolConfig().addNode(zuliaRestoreArgs.address, zuliaRestoreArgs.port).setNodeUpdateEnabled(false);
			ZuliaWorkPool workPool = new ZuliaWorkPool(zuliaPoolConfig);

			String dir = zuliaRestoreArgs.dir;
			String index = zuliaRestoreArgs.index;
			String idField = zuliaRestoreArgs.idField;
			Boolean drop = zuliaRestoreArgs.drop;

			if (index != null) {
				// restore only this index
				restore(workPool, dir, index, idField, drop);
			}
			else {
				// walk dir and restore everything
				Files.list(Paths.get(dir)).forEach(indexDir -> {
					try {
						String ind = indexDir.getFileName().toString();
						restore(workPool, dir, ind, idField, drop);
					}
					catch (Exception e) {
						LOG.log(Level.SEVERE, "There was a problem restoring index <" + indexDir.getFileName() + ">", e);
					}

				});
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

	private static void restore(ZuliaWorkPool workPool, String dir, String index, String idField, boolean drop) throws Exception {
		String recordsFilename = dir + File.separator + index + File.separator + index + ".json";
		String settingsFilename = dir + File.separator + index + File.separator + index + "_settings.json";

		if (Files.exists(Paths.get(settingsFilename)) && Files.exists(Paths.get(recordsFilename))) {
			if (drop) {
				workPool.deleteIndex(index);
			}

			LOG.info("Creating index <" + index + ">");
			ZuliaIndex.IndexSettings.Builder indexSettingsBuilder = ZuliaIndex.IndexSettings.newBuilder();
			JsonFormat.parser().merge(Files.readString(Paths.get(settingsFilename), Charsets.UTF_8), indexSettingsBuilder);
			ClientIndexConfig indexConfig = new ClientIndexConfig();
			indexConfig.configure(indexSettingsBuilder.build());
			workPool.createIndex(indexConfig);
			LOG.info("Finished creating index <" + index + ">");

			AtomicInteger count = new AtomicInteger();
			LOG.info("Starting to index records for index <" + index + ">");
			ZuliaCmdUtil.index(recordsFilename, idField, index, workPool, count);
			LOG.info("Finished indexing for index <" + index + "> with total records: " + count);
		}
		else {
			if (index.endsWith(".json")) {
				System.err.println("Please provide the path to the parent directory in --dir option.");
				System.exit(9);
			}
			else {
				System.err.println("Index <" + index + "> does not exist in the given dir <" + dir + ">");
				System.exit(9);
			}
		}

	}

}
