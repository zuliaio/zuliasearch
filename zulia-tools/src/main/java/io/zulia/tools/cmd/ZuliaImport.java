package io.zulia.tools.cmd;

import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.cmd.common.ShowStackArgs;
import io.zulia.cmd.common.ZuliaCommonCmd;
import io.zulia.cmd.common.ZuliaVersionProvider;
import io.zulia.tools.cmd.common.SingleIndexArgs;
import io.zulia.tools.cmd.common.ThreadedArgs;
import io.zulia.tools.cmd.common.ZuliaCmdUtil;
import io.zulia.tools.cmd.zuliaadmin.ConnectionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;

@CommandLine.Command(name = "zuliaimport", versionProvider = ZuliaVersionProvider.class, scope = CommandLine.ScopeType.INHERIT)
public class ZuliaImport implements Callable<Integer> {

	private static final Logger LOG = LoggerFactory.getLogger(ZuliaImport.class);
	@CommandLine.Mixin
	private ConnectionInfo connectionInfo;

	@CommandLine.Mixin
	private ShowStackArgs showStackArgs;

	@CommandLine.Mixin
	private ThreadedArgs threadedArgs;

	@CommandLine.Mixin
	private SingleIndexArgs singleIndexArgs;

	@CommandLine.Option(names = { "--idField" }, description = "ID field name, defaults to 'id'")
	private String idField = "id";

	@CommandLine.Option(names = "--dir", description = "Full path to the zuliaexport directory.", required = true)
	private String dir;

	@Override
	public Integer call() throws Exception {
		ZuliaWorkPool zuliaWorkPool = connectionInfo.getConnection();

		String index = singleIndexArgs.getIndex();

		String inputDir = dir + File.separator + index;
		String recordsFilename = inputDir + File.separator + index + ".json";

		if (Files.exists(Paths.get(recordsFilename))) {
			AtomicInteger count = new AtomicInteger();
			LOG.info("Starting to index records for index {}", index);
			ZuliaCmdUtil.index(inputDir, recordsFilename, idField, index, zuliaWorkPool, count, threadedArgs.getThreads(),
					ZuliaCmdUtil.AssociatedFilesHandling.skip);
			LOG.info("Finished indexing for index {} with total records: {}", index, count);
		}
		else {
			System.err.println("File " + recordsFilename + " does not exist in the given dir " + dir);
			System.exit(9);
		}

		return CommandLine.ExitCode.OK;
	}

	public static void main(String[] args) {

		ZuliaCommonCmd.runCommandLine(new ZuliaImport(), args);
	}

}
