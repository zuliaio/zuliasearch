package io.zulia.tools.cmd;

import com.google.common.base.Charsets;
import com.google.protobuf.util.JsonFormat;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.cmd.common.ShowStackArgs;
import io.zulia.cmd.common.ZuliaCommonCmd;
import io.zulia.cmd.common.ZuliaVersionProvider;
import io.zulia.message.ZuliaIndex;
import io.zulia.tools.cmd.common.ThreadedArgs;
import io.zulia.tools.cmd.common.ZuliaCmdUtil;
import io.zulia.tools.cmd.zuliaadmin.ConnectionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

@CommandLine.Command(name = "zuliarestore", versionProvider = ZuliaVersionProvider.class, scope = CommandLine.ScopeType.INHERIT)
public class ZuliaRestore implements Callable<Integer> {

	private static final Logger LOG = LoggerFactory.getLogger(ZuliaRestore.class);
	@CommandLine.Mixin
	private ConnectionInfo connectionInfo;

	@CommandLine.Mixin
	private ShowStackArgs showStackArgs;

	@CommandLine.Mixin
	private ThreadedArgs threadedArgs;

	//Not sharded with MultipleIndexArgs because wildcard is not against zulia existing indexes and it is not required
	@CommandLine.Option(names = { "-i", "--indexes",
			"--index" }, paramLabel = "index", description = "Index name. For multiple indexes, repeat arg or use commas to separate within a single arg", split = ",")
	private Collection<String> indexes;

	@CommandLine.Option(names = { "-o", "--out" }, description = "Full path to the output directory. (default: ${DEFAULT-VALUE})")
	private String out = System.getProperty("user.dir");

	@CommandLine.Option(names = "--idField", description = "Id field name (default: ${DEFAULT-VALUE})")
	private String idField = "id";

	@CommandLine.Option(names = "--dir", description = "Full path to the zuliadump directory", required = true)
	private String dir;

	@CommandLine.Option(names = "--drop", description = "Drop the index before restoring (default: ${DEFAULT-VALUE})")
	private Boolean drop = false;

	@CommandLine.Option(names = { "-a",
			"--associatedFilesHandling" }, description = "Options for handling associated files if included in the dump. Options: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE})")
	public ZuliaCmdUtil.AssociatedFilesHandling associatedFilesHandling = ZuliaCmdUtil.AssociatedFilesHandling.skip;

	@Override
	public Integer call() throws Exception {
		ZuliaWorkPool zuliaWorkPool = connectionInfo.getConnection();

		int threads = threadedArgs.getThreads();

		if (indexes != null) {
			for (String index : indexes) {
				restore(zuliaWorkPool, dir, index, idField, drop, threads, associatedFilesHandling);
			}
		}
		else {
			// walk dir and restore everything
			try (Stream<Path> list = Files.list(Paths.get(dir))) {
				for (Path indexDir : list.toList()) {
					try {
						String ind = indexDir.getFileName().toString();
						restore(zuliaWorkPool, dir, ind, idField, drop, threads, associatedFilesHandling);
					}
					catch (Exception e) {
						throw new Exception("There was a problem restoring index <" + indexDir.getFileName() + ">");
					}
				}
			}
		}

		return CommandLine.ExitCode.OK;
	}

	private static void restore(ZuliaWorkPool workPool, String dir, String index, String idField, Boolean drop, Integer threads,
			ZuliaCmdUtil.AssociatedFilesHandling associatedFilesHandling) throws Exception {
		String inputDir = dir + File.separator + index;
		String recordsFilename = inputDir + File.separator + index + ".json";
		String settingsFilename = inputDir + File.separator + index + "_settings.json";

		Path settingsPath = Paths.get(settingsFilename);
		if (Files.exists(settingsPath) && Files.exists(Paths.get(recordsFilename))) {
			if (drop) {
				workPool.deleteIndex(index);
			}

			LOG.info("Creating index <" + index + ">");
			ZuliaIndex.IndexSettings.Builder indexSettingsBuilder = ZuliaIndex.IndexSettings.newBuilder();
			JsonFormat.parser().merge(Files.readString(settingsPath, Charsets.UTF_8), indexSettingsBuilder);
			ClientIndexConfig indexConfig = new ClientIndexConfig();
			indexConfig.configure(indexSettingsBuilder.build());
			workPool.createIndex(indexConfig);
			LOG.info("Finished creating index <" + index + ">");

			AtomicInteger count = new AtomicInteger();
			LOG.info("Starting to index records for index <" + index + ">");
			ZuliaCmdUtil.index(inputDir, recordsFilename, idField, index, workPool, count, threads, associatedFilesHandling);
			LOG.info("Finished indexing for index <" + index + "> with total records: " + count);
		}
		else {
			if (index.endsWith(".json")) {
				throw new Exception("Please provide the path to the parent directory in --dir option.");
			}
			else {
				throw new Exception("Index <" + index + "> does not exist in the given dir <" + dir
						+ ">, please provide the path to the parent directory in --dir option.");
			}
		}
	}

	public static void main(String[] args) {

		ZuliaCommonCmd.runCommandLine(new ZuliaRestore(), args);
	}

}
