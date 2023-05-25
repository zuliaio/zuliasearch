package io.zulia.server.connection.server;

import io.grpc.Metadata;
import io.grpc.Server;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.zulia.server.config.ZuliaConfig;
import io.zulia.server.index.ZuliaIndexManager;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Created by Matt Davis on 6/28/17.
 *
 * @author mdavis
 */
public class ZuliaServiceServer {

	public static class ResponseCompressionIntercept implements ServerInterceptor {

		@Override
		public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call, Metadata headers, ServerCallHandler<ReqT, RespT> next) {
			call.setCompression("gzip");
			return next.startCall(call, headers);
		}
	}

	private Server server;

	public ZuliaServiceServer(ZuliaConfig zuliaConfig, ZuliaIndexManager indexManager) {

		int externalServicePort = zuliaConfig.getServicePort();

		ZuliaServiceHandler zuliaServiceHandler = new ZuliaServiceHandler(indexManager);
		NettyServerBuilder nettyServerBuilder = NettyServerBuilder.forPort(externalServicePort).addService(zuliaServiceHandler)
				.maxInboundMessageSize(128 * 1024 * 1024);

		if (zuliaConfig.isResponseCompression()) {
			nettyServerBuilder = nettyServerBuilder.intercept(new ResponseCompressionIntercept());
		}
		server = nettyServerBuilder.build();
	}

	public void start() throws IOException {
		server.start();
	}

	public void shutdown() {
		server.shutdown();
		try {
			server.awaitTermination(2, TimeUnit.SECONDS);
		}
		catch (InterruptedException ignored) {

		}
	}
}
