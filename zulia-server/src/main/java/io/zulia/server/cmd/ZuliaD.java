package io.zulia.server.cmd;

import io.zulia.cmd.common.ShowStackArgs;
import io.zulia.cmd.common.ZuliaCommonCmd;
import io.zulia.cmd.common.ZuliaVersionProvider;
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
		if (!configPath.startsWith(File.separator)) {
			String prefix = System.getenv("APP_HOME");
			if (prefix != null) {
				return prefix + File.separator + configPath;
			}
		}

		return configPath;
	}

	public static void main(String[] args) {
		System.out.println("APP_HOME is set to " + System.getenv("APP_HOME"));
		ZuliaCommonCmd.runCommandLine(new ZuliaD(), args);

	}

}
