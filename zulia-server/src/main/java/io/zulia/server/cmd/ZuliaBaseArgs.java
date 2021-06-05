package io.zulia.server.cmd;

import com.beust.jcommander.Parameter;

public class ZuliaBaseArgs {

	@Parameter(names = "--help", help = true)
	public boolean help;

	@Parameter(names = "--address", description = "Zulia Server Address", order = 1)
	public String address = "localhost";

	@Parameter(names = "--port", description = "Zulia Port", order = 2)
	public Integer port = 32191;

	@Parameter(names = "--index", description = "Index name")
	public String index;

	@Parameter(names = "--skipExistingFiles", description = "Skip storing files if they already exist.")
	public Boolean skipExistingFiles = false;

	@Parameter(names = "--threads", description = "Number of threads to use for restoring.")
	public Integer threads = 4;
}
