package io.zulia.server.cmd.zuliaadmin;

import io.zulia.client.config.ZuliaPoolConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import picocli.CommandLine;

public class ConnectionInfo {

	@CommandLine.Option(names = {"--address","--server"} , description = "Zulia Server Address")
	public String address = "localhost";

	@CommandLine.Option(names = "--port", description = "Zulia Port")
	public Integer port = 32191;

	@CommandLine.Option(names = "--routing", description = "Use smart routing to route request to the correct node (do not use with ssh port forwarding)")
	public boolean routingEnabled;

	public ZuliaWorkPool getConnection() throws Exception {
		ZuliaPoolConfig config = new ZuliaPoolConfig().addNode(address, port).setRoutingEnabled(routingEnabled);
		return new ZuliaWorkPool(config);
	}

}
