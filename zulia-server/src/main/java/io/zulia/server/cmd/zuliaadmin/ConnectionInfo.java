package io.zulia.server.cmd.zuliaadmin;

import io.zulia.client.config.ZuliaPoolConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import picocli.CommandLine;

public class ConnectionInfo {

	@CommandLine.Option(names = {"--address","--server"} , description = "Zulia Server Address")
	public String address = "localhost";

	@CommandLine.Option(names = "--port", description = "Zulia Port")
	public Integer port = 32191;

	public ZuliaWorkPool getConnection() throws Exception {
		ZuliaPoolConfig config = new ZuliaPoolConfig().addNode(address, port).setRoutingEnabled(false);
		return new ZuliaWorkPool(config);
	}

}
