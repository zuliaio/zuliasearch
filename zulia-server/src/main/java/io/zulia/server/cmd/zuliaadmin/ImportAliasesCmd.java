package io.zulia.server.cmd.zuliaadmin;

import com.google.protobuf.util.JsonFormat;
import io.zulia.client.command.CreateIndexAlias;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.message.ZuliaIndex;
import io.zulia.server.cmd.ZuliaAdmin;
import picocli.CommandLine;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "importAliases", description = "Import aliases from file")
public class ImportAliasesCmd implements Callable<Integer> {

    @CommandLine.ParentCommand
    private ZuliaAdmin zuliaAdmin;

    @CommandLine.Option(names = {"-f", "--file"}, description = "Input file to read JSON import from", required = true)
    private File inputFile;

    @Override
    public Integer call() throws Exception {

        ZuliaWorkPool zuliaWorkPool = zuliaAdmin.getConnection();

        List<String> aliases = Files.readAllLines(inputFile.toPath());

        for (String alias : aliases) {
            ZuliaIndex.IndexAlias.Builder indexAliasBuilder = ZuliaIndex.IndexAlias.newBuilder();
            JsonFormat.parser().merge(alias, indexAliasBuilder);
            zuliaWorkPool.createIndexAlias(new CreateIndexAlias(indexAliasBuilder.build()));
        }

        return CommandLine.ExitCode.OK;
    }
}
