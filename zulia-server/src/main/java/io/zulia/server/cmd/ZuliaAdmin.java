package io.zulia.server.cmd;

import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.server.cmd.common.ShowStackArgs;
import io.zulia.server.cmd.common.ZuliaVersionProvider;
import io.zulia.server.cmd.zuliaadmin.ClearIndexCmd;
import io.zulia.server.cmd.zuliaadmin.ConnectionInfo;
import io.zulia.server.cmd.zuliaadmin.CreateAliasCmd;
import io.zulia.server.cmd.zuliaadmin.DeleteAliasCmd;
import io.zulia.server.cmd.zuliaadmin.DeleteIndexCmd;
import io.zulia.server.cmd.zuliaadmin.DisplayAliasesCmd;
import io.zulia.server.cmd.zuliaadmin.DisplayIndexesCmd;
import io.zulia.server.cmd.zuliaadmin.DisplayNodesCmd;
import io.zulia.server.cmd.zuliaadmin.DocCountCmd;
import io.zulia.server.cmd.zuliaadmin.ExportAliasesCmd;
import io.zulia.server.cmd.zuliaadmin.ImportAliasesCmd;
import io.zulia.server.cmd.zuliaadmin.OptimizeIndexCmd;
import io.zulia.server.cmd.zuliaadmin.ReindexCmd;
import picocli.CommandLine;

@CommandLine.Command(name = "zuliaadmin", subcommands = { DisplayNodesCmd.class, DisplayIndexesCmd.class, DocCountCmd.class, ClearIndexCmd.class,
		DeleteIndexCmd.class, OptimizeIndexCmd.class, ReindexCmd.class, CreateAliasCmd.class, DeleteAliasCmd.class, DisplayAliasesCmd.class,
		ExportAliasesCmd.class,
		ImportAliasesCmd.class, }, mixinStandardHelpOptions = true, versionProvider = ZuliaVersionProvider.class, scope = CommandLine.ScopeType.INHERIT)
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
