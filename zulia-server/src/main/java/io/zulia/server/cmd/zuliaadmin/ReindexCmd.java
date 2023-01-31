package io.zulia.server.cmd.zuliaadmin;

import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.server.cmd.ZuliaAdmin;
import io.zulia.server.cmd.common.MultipleIndexArgs;
import picocli.CommandLine;

import java.util.Set;
import java.util.concurrent.Callable;

@CommandLine.Command(name = "reindex", description = "Reindexes index(es) specified by --index.  Reads document from current index and reindexes with current schema")
public class ReindexCmd implements Callable<Integer> {

    @CommandLine.ParentCommand
    private ZuliaAdmin zuliaAdmin;

    @CommandLine.Mixin
    private MultipleIndexArgs multipleIndexArgs;

    @Override
    public Integer call() throws Exception {

        ZuliaWorkPool zuliaWorkPool = zuliaAdmin.getConnection();

        Set<String> indexes = multipleIndexArgs.resolveIndexes(zuliaWorkPool);

        for (String index : indexes) {
            zuliaWorkPool.reindex(index);
        }
        return CommandLine.ExitCode.OK;
    }
}
