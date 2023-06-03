package io.zulia.server.cmd.zuliaadmin;

import io.zulia.client.config.ZuliaPoolConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import picocli.CommandLine;

public class ConnectionInfo {

	@CommandLine.Option(names = { "--address", "--server" }, description = "Zulia Server Address")
	public String address = System.getenv("ZULIA_HOST") != null ? System.getenv("ZULIA_HOST") : "localhost";

	@CommandLine.Option(names = "--port", description = "Zulia Port")
	public Integer port = System.getenv("ZULIA_PORT") != null ? Integer.parseInt(System.getenv("ZULIA_PORT")) : 32191;

	@CommandLine.Option(names = "--routing", description = "Use smart routing to route request to the correct node (do not use with ssh port forwarding)")
	public boolean routingEnabled;

	public ZuliaWorkPool getConnection() throws Exception {
		ZuliaPoolConfig config = new ZuliaPoolConfig().addNode(address, port).setRoutingEnabled(routingEnabled).setNodeUpdateEnabled(routingEnabled);
		return new ZuliaWorkPool(config);
	}

}
