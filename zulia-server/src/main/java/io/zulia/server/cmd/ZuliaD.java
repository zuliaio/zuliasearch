package io.zulia.server.cmd;

import io.zulia.log.LogUtil;
import io.zulia.server.cmd.common.SelectiveStackTraceHandler;
import io.zulia.server.cmd.common.ShowStackArgs;
import io.zulia.server.cmd.common.ZuliaVersionProvider;
import io.zulia.server.cmd.zuliad.AddNodeCmd;
import io.zulia.server.cmd.zuliad.ListNodesCmd;
import io.zulia.server.cmd.zuliad.RemoveNodeCmd;
import io.zulia.server.cmd.zuliad.StartNodeCmd;
import picocli.CommandLine;

import java.io.File;

@CommandLine.Command(name = "zuliad", subcommands = { AddNodeCmd.class, ListNodesCmd.class, RemoveNodeCmd.class,
		StartNodeCmd.class }, mixinStandardHelpOptions = true, versionProvider = ZuliaVersionProvider.class, scope = CommandLine.ScopeType.INHERIT)
public class ZuliaD {

	@CommandLine.Option(names = "--config", description = "Full path to the config (defaults to $APP_HOME/config/zulia.properties)", scope = CommandLine.ScopeType.INHERIT)
	private String configPath = "config" + File.separator + "zulia.properties";

	@CommandLine.Mixin
	private ShowStackArgs showStackArgs;

	public String getConfigPath() {
		return configPath;
	}

	public static void main(String[] args) {

		LogUtil.init();
		ZuliaD zuliaD = new ZuliaD();
		int exitCode = new CommandLine(zuliaD).setAbbreviatedSubcommandsAllowed(true).setAbbreviatedOptionsAllowed(true)
				.setExecutionExceptionHandler(new SelectiveStackTraceHandler()).execute(args);

		// dont kill the start node daemon, java will exit 0 for us in other cases
		if (exitCode != 0) {
			System.exit(exitCode);
		}

	}

}
