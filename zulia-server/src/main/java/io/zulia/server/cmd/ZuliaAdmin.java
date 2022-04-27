package io.zulia.server.cmd;

import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.log.LogUtil;
import io.zulia.server.cmd.common.SelectiveStackTraceHandler;
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
import io.zulia.server.cmd.zuliaadmin.OptimizeIndexCmd;
import io.zulia.server.cmd.zuliaadmin.ReindexCmd;
import picocli.CommandLine;

@CommandLine.Command(name = "zuliaadmin", subcommands = { ClearIndexCmd.class, DisplayIndexesCmd.class, DeleteIndexCmd.class, OptimizeIndexCmd.class,
		ReindexCmd.class, DisplayNodesCmd.class, CreateAliasCmd.class, DeleteAliasCmd.class, DisplayAliasesCmd.class,
		DocCountCmd.class }, mixinStandardHelpOptions = true, versionProvider = ZuliaVersionProvider.class, scope = CommandLine.ScopeType.INHERIT)
public class ZuliaAdmin {

	@CommandLine.Mixin
	private ConnectionInfo connectionInfo;

	@CommandLine.Mixin
	private ShowStackArgs showStackArgs;

	public ZuliaWorkPool getConnection() throws Exception {
		return connectionInfo.getConnection();
	}

	public static void main(String[] args) {

		LogUtil.init();
		ZuliaAdmin zuliaAdmin = new ZuliaAdmin();
		int exitCode = new CommandLine(zuliaAdmin).setAbbreviatedSubcommandsAllowed(true).setAbbreviatedOptionsAllowed(true)
				.setExecutionExceptionHandler(new SelectiveStackTraceHandler()).execute(args);
		System.exit(exitCode);
	}

}
