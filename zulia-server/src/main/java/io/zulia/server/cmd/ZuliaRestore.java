package io.zulia.server.cmd;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Charsets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.protobuf.util.JsonFormat;
import io.zulia.client.command.Store;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.config.ZuliaPoolConfig;
import io.zulia.client.pool.WorkPool;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.log.LogUtil;
import io.zulia.message.ZuliaIndex;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Date;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ZuliaRestore {

	private static final Logger LOG = Logger.getLogger(ZuliaDump.class.getSimpleName());

	private static final Gson GSON = new GsonBuilder().create();

	public static class ZuliaRestoreArgs {

		@Parameter(names = "--help", help = true)
		private boolean help;

		@Parameter(names = "--address", description = "Zulia Server Address", order = 1)
		private String address = "localhost";

		@Parameter(names = "--port", description = "Zulia Port", order = 2)
		private Integer port = 32191;

		@Parameter(names = "--index", description = "Index name to dump.")
		private String index;

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

			WorkPool threadPool = new WorkPool(4);
			ZuliaPoolConfig zuliaPoolConfig = new ZuliaPoolConfig().addNode(zuliaRestoreArgs.address, zuliaRestoreArgs.port);
			ZuliaWorkPool workPool = new ZuliaWorkPool(zuliaPoolConfig);

			String dir = zuliaRestoreArgs.dir;
			String index = zuliaRestoreArgs.index;
			Boolean drop = zuliaRestoreArgs.drop;

			if (index != null) {
				// restore only this index
				restore(threadPool, workPool, dir, index, drop);
			}
			else {
				// walk dir and restore everything
				Files.list(Paths.get(dir)).forEach(indexDir -> {
					try {
						String ind = indexDir.getFileName().toString();
						restore(threadPool, workPool, dir, ind, drop);
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

	private static void restore(WorkPool threadPool, ZuliaWorkPool workPool, String dir, String index, boolean drop) throws Exception {
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

			LOG.info("Starting to index records for index <" + index + ">");
			AtomicInteger count = new AtomicInteger();
			try (BufferedReader b = new BufferedReader(new FileReader(recordsFilename))) {
				String line;
				while ((line = b.readLine()) != null) {
					final String record = line;
					threadPool.executeAsync((Callable<Void>) () -> {
						Document document = Document.parse(record);
						String id = document.getString("id");
						if (id == null) {
							if (document.get("id") instanceof String) {
								id = document.getString("id");
							}
						}

						if (id == null) {
							throw new RuntimeException("No id for record: " + document.toJson());
						}

						document.put("indexTime", new Date());

						Store store = new Store(id, index);
						store.setResultDocument(new ResultDocBuilder().setDocument(document));
						workPool.store(store);

						if (count.incrementAndGet() % 10000 == 0) {
							LOG.info("So far indexed <" + count + "> for index <" + index + ">");
						}
						return null;
					});
				}
			}
			LOG.info("Finished indexing for index <" + index + "> with total records: " + count);
		}
		else {
			System.err.println("Index <" + index + "> does not exist in the given dir <" + dir + ">");
			System.exit(9);
		}

	}

}
