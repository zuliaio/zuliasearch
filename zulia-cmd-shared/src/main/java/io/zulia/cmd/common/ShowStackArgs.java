package io.zulia.cmd.common;

import picocli.CommandLine;

public class ShowStackArgs {

	@CommandLine.Option(names = { "--showStack", }, description = "Show stack trace on failure", scope = CommandLine.ScopeType.INHERIT)
	private boolean showStack;

	public boolean isShowStack() {
		return showStack;
	}
}
