package io.zulia.tools.cmd.zuliaadmin;

import com.google.common.base.Charsets;
import com.google.protobuf.util.JsonFormat;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.message.ZuliaIndex;
import io.zulia.tools.cmd.ZuliaAdmin;
import io.zulia.tools.cmd.common.AliasArgs;
import picocli.CommandLine;

import java.io.File;
import java.io.FileWriter;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "exportAliases", description = "Exports aliases to a file given by --alias")
public class ExportAliasesCmd implements Callable<Integer> {

	@CommandLine.ParentCommand
	private ZuliaAdmin zuliaAdmin;

	@CommandLine.Mixin
	private AliasArgs aliasArgs;

	@CommandLine.Option(names = { "-f", "--file" }, description = "Output file to save the JSON export", required = true)
	private File outputFile;

	@Override
	public Integer call() throws Exception {

		ZuliaWorkPool zuliaWorkPool = zuliaAdmin.getConnection();

		List<ZuliaIndex.IndexAlias> indexAliases = zuliaWorkPool.getNodes().getIndexAliases();

		Set<String> aliases = aliasArgs.resolveAliases(zuliaWorkPool);

		try (FileWriter fileWriter = new FileWriter(outputFile, Charsets.UTF_8)) {
			for (ZuliaIndex.IndexAlias indexAlias : indexAliases) {
				if (aliases.contains(indexAlias.getAliasName())) {
					JsonFormat.Printer printer = JsonFormat.printer().omittingInsignificantWhitespace();
					fileWriter.write(printer.print(indexAlias) + "\n");
				}
			}
		}

		return CommandLine.ExitCode.OK;
	}
}
