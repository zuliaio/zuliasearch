package io.zulia.server.cmd;

import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.server.cmd.common.ShowStackArgs;
import io.zulia.server.cmd.common.ZuliaVersionProvider;
import io.zulia.server.cmd.zuliaadmin.*;
import picocli.CommandLine;

@CommandLine.Command(name = "zuliaadmin", subcommands = {DisplayNodesCmd.class, DisplayIndexesCmd.class, DocCountCmd.class, ClearIndexCmd.class,
        DeleteIndexCmd.class, OptimizeIndexCmd.class, ReindexCmd.class, CreateAliasCmd.class, DeleteAliasCmd.class, DisplayAliasesCmd.class,
        ExportAliasesCmd.class,
        ImportAliasesCmd.class, StoreAssociatedFileCmd.class, FetchAssociatedFileCmd.class}, mixinStandardHelpOptions = true, versionProvider = ZuliaVersionProvider.class, scope = CommandLine.ScopeType.INHERIT)
public class ZuliaAdmin {

    @CommandLine.Mixin
    private ConnectionInfo connectionInfo;

    @CommandLine.Mixin
    private ShowStackArgs showStackArgs;

    public ZuliaWorkPool getConnection() throws Exception {
        return connectionInfo.getConnection();
    }

    public static void main(String[] args) {
        ZuliaCommonCmd.runCommandLine(new ZuliaAdmin(), args);
    }

}
