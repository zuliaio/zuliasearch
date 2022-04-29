package io.zulia.server.cmd.common;

import picocli.CommandLine;

public class ThreadedArgs {

	@CommandLine.Option(names = { "--threads", }, description = "Number of threads to use", scope = CommandLine.ScopeType.INHERIT)
	private int threads = 4;

	public int getThreads() {
		return threads;
	}
}
