package io.zulia.server.cmd;

import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.server.cmd.common.MultipleIndexArgs;
import io.zulia.server.cmd.common.ShowStackArgs;
import io.zulia.server.cmd.common.ZuliaCmdUtil;
import io.zulia.server.cmd.common.ZuliaVersionProvider;
import io.zulia.server.cmd.zuliaadmin.ConnectionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

@CommandLine.Command(name = "zuliaexport", versionProvider = ZuliaVersionProvider.class, scope = CommandLine.ScopeType.INHERIT)
public class ZuliaExport implements Callable<Integer> {

	private static final Logger LOG = LoggerFactory.getLogger(ZuliaExport.class);
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


	@Override
	public Integer call() throws Exception {



		ZuliaWorkPool zuliaWorkPool = connectionInfo.getConnection();

		Set<String> indexes = multipleIndexArgs.resolveIndexes(zuliaWorkPool);
		for (String ind : indexes) {
			queryAndWriteOutput(zuliaWorkPool, ind, q, pageSize, out);
		}

		return CommandLine.ExitCode.OK;
	}

	private static void queryAndWriteOutput(ZuliaWorkPool workPool, String index, String q, Integer pageSize, String out) throws Exception {
		queryAndWriteOutput(workPool, index, q, pageSize, out, null);
	}

	private static void queryAndWriteOutput(ZuliaWorkPool workPool, String index, String q, Integer pageSize, String out, Set<String> uniqueIds)
			throws Exception {

		// create zuliaexport dir first
		String zuliaExportDir = out + File.separator + "zuliaexport";
		Path zuliaExportPath = Paths.get(zuliaExportDir);
		if (!Files.exists(zuliaExportPath)) {
			Files.createDirectory(zuliaExportPath);
		}

		// create index dir
		String indOutputDir = zuliaExportDir + File.separator + index;
		Path indOutputPath = Paths.get(indOutputDir);
		if (!Files.exists(indOutputPath)) {
			Files.createDirectory(indOutputPath);
		}

		String recordsFilename = indOutputDir + File.separator + index + ".json";

		AtomicInteger count = new AtomicInteger();
		LOG.info("Exporting from index <" + index + ">");
		ZuliaCmdUtil.writeOutput(recordsFilename, index, q, pageSize, workPool, count, uniqueIds);
		LOG.info("Finished exporting from index <" + index + ">, total: " + count);

	}

	public static void main(String[] args) {

		ZuliaCommonCmd.runCommandLine(new ZuliaExport(), args);
	}

}
