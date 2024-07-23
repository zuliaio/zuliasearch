package io.zulia.tools.cmd;

import com.google.common.base.Charsets;
import com.google.protobuf.util.JsonFormat;
import io.zulia.client.command.FetchLargeAssociated;
import io.zulia.client.command.GetIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.cmd.common.ShowStackArgs;
import io.zulia.cmd.common.ZuliaCommonCmd;
import io.zulia.cmd.common.ZuliaVersionProvider;
import io.zulia.tools.cmd.common.MultipleIndexArgs;
import io.zulia.tools.cmd.common.ZuliaCmdUtil;
import io.zulia.tools.cmd.zuliaadmin.ConnectionInfo;
import io.zulia.util.pool.TaskExecutor;
import io.zulia.util.pool.WorkPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.File;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

@CommandLine.Command(name = "zuliadump", versionProvider = ZuliaVersionProvider.class, scope = CommandLine.ScopeType.INHERIT)
public class ZuliaDump implements Callable<Integer> {

	private static final Logger LOG = LoggerFactory.getLogger(ZuliaDump.class);
	@CommandLine.Mixin
	private ConnectionInfo connectionInfo;

	@CommandLine.Mixin
	private ShowStackArgs showStackArgs;

	@CommandLine.Mixin
	private MultipleIndexArgs multipleIndexArgs;

	@CommandLine.Option(names = { "-o", "--out" }, description = "Full path to the output directory. (default: ${DEFAULT-VALUE})")
	private String out = System.getProperty("user.dir");

	@CommandLine.Option(names = { "-q", "--query" }, description = "Zulia query, matches all docs by default (default: ${DEFAULT-VALUE})")
	private String q = "*:*";

	@CommandLine.Option(names = { "-p", "--pageSize", "--rows" }, description = "Number of records in each page (default: ${DEFAULT-VALUE})")
	private Integer pageSize = 1000;

	@CommandLine.Option(names = { "-a", "--includeAssociatedDocs" }, description = "Include Associated Documents in the dump (default: ${DEFAULT-VALUE})")
	private boolean includeAssociatedDocs = false;

	@Override
	public Integer call() throws Exception {
		ZuliaWorkPool zuliaWorkPool = connectionInfo.getConnection();

		Set<String> indexes = multipleIndexArgs.resolveIndexes(zuliaWorkPool);
		for (String ind : indexes) {
			Set<String> uniqueIds = new HashSet<>();
			queryAndWriteOutput(zuliaWorkPool, ind, q, pageSize, out, uniqueIds);
			if (includeAssociatedDocs) {
				fetchAssociatedDocs(zuliaWorkPool, ind, out, uniqueIds);
			}
		}

		return CommandLine.ExitCode.OK;
	}

	private static void queryAndWriteOutput(ZuliaWorkPool workPool, String index, String q, Integer pageSize, String outputDir, Set<String> uniqueIds)
			throws Exception {

		// create zuliadump dir first
		String zuliaDumpDir = outputDir + File.separator + "zuliadump";
		Path zuliaDumpDirPath = Paths.get(zuliaDumpDir);
		if (!Files.exists(zuliaDumpDirPath)) {
			Files.createDirectories(zuliaDumpDirPath);
		}

		// create index dir
		String indOutputDir = zuliaDumpDir + File.separator + index;
		Path indOutputDirPath = Paths.get(indOutputDir);
		if (!Files.exists(indOutputDirPath)) {
			Files.createDirectory(indOutputDirPath);
		}

		String recordsFilename = indOutputDir + File.separator + index + ".json";
		String settingsFilename = indOutputDir + File.separator + index + "_settings.json";

		AtomicInteger count = new AtomicInteger();
		LOG.info("Dumping index <{}>", index);
		ZuliaCmdUtil.writeOutput(recordsFilename, index, q, pageSize, workPool, count, uniqueIds);
		LOG.info("Finished dumping index <{}>, total: {}", index, count);

		try (FileWriter fileWriter = new FileWriter(settingsFilename, Charsets.UTF_8)) {
			LOG.info("Writing settings for index <{}>", index);
			JsonFormat.Printer printer = JsonFormat.printer();
			fileWriter.write(printer.print(workPool.getIndexConfig(new GetIndexConfig(index)).getIndexConfig().getIndexSettings()));
			LOG.info("Finished writing settings for index <{}>", index);
		}

	}

	private static void fetchAssociatedDocs(ZuliaWorkPool workPool, String index, String outputDir, Set<String> uniqueIds) throws Exception {

		String zuliaDumpDir = outputDir + File.separator + "zuliadump";
		String indOutputDir = zuliaDumpDir + File.separator + index;

		LOG.info("Starting to dump associated docs for <{}> documents", uniqueIds.size());
		AtomicInteger count = new AtomicInteger(0);
		try (TaskExecutor threadPool = WorkPool.nativePool(4)) {
			for (String uniqueId : uniqueIds) {
				threadPool.executeAsync(() -> {

					workPool.fetchLargeAssociated(new FetchLargeAssociated(uniqueId, index,
							Paths.get(indOutputDir + File.separator + uniqueId.replaceAll("/", "_") + ".zip").toFile()));
					if (count.incrementAndGet() % 1000 == 0) {
						LOG.info("Associated docs dumped so far: {}", count);
					}

					return null;
				});
			}
			LOG.info("Finished dumping associated docs for <{}> documents", uniqueIds.size());
		}
	}

	public static void main(String[] args) {

		ZuliaCommonCmd.runCommandLine(new ZuliaDump(), args);
	}

}
