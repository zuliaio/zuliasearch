package io.zulia.server.connection.server;

import io.grpc.Server;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.zulia.server.config.ZuliaConfig;
import io.zulia.server.index.ZuliaIndexManager;

import java.io.IOException;

/**
 * Created by Matt Davis on 6/28/17.
 * @author mdavis
 */
public class ZuliaServiceServer {

	private Server server;

	public ZuliaServiceServer(ZuliaConfig zuliaConfig, ZuliaIndexManager indexManager) {

		int externalServicePort = zuliaConfig.getServicePort();

		ZuliaServiceHandler zuliaServiceHandler = new ZuliaServiceHandler(indexManager);
		server = NettyServerBuilder.forPort(externalServicePort).addService(zuliaServiceHandler).maxInboundMessageSize(128 * 1024 * 1024).build();
	}

	public void start() throws IOException {
		server.start();
	}

	public void shutdown() {
		server.shutdown();

	}
}
