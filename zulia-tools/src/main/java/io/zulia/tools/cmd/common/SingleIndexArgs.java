package io.zulia.tools.cmd.common;

import picocli.CommandLine;

public class SingleIndexArgs {

	@CommandLine.Option(names = { "-i", "--index" }, description = "Index name", required = true)
	private String index;

	public String getIndex() {
		return index;
	}
}
