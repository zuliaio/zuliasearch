package io.zulia.client.pool;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.zulia.message.ZuliaBase.Node;
import io.zulia.message.ZuliaServiceGrpc;

import java.util.concurrent.TimeUnit;

public class ZuliaConnection {

	private final boolean compressedConnection;
	private final Node node;
	private ManagedChannel channel;
	private ZuliaServiceGrpc.ZuliaServiceBlockingStub blockingStub;
	private ZuliaServiceGrpc.ZuliaServiceStub asyncStub;

	public ZuliaConnection(Node node, boolean compressedConnection) {
		this.node = node;
		this.compressedConnection = compressedConnection;
	}

	public void open() {

		ManagedChannelBuilder<?> managedChannelBuilder = ManagedChannelBuilder.forAddress(node.getServerAddress(), node.getServicePort())
				.maxInboundMessageSize(256 * 1024 * 1024).usePlaintext().keepAliveTime(30, TimeUnit.SECONDS).keepAliveTimeout(5, TimeUnit.SECONDS)
				.keepAliveWithoutCalls(true);
		channel = managedChannelBuilder.build();

		blockingStub = ZuliaServiceGrpc.newBlockingStub(channel);
		if (compressedConnection) {
			blockingStub = blockingStub.withCompression("gzip");
		}

		asyncStub = ZuliaServiceGrpc.newStub(channel);
		if (compressedConnection) {
			asyncStub = asyncStub.withCompression("gzip");
		}

	}

	public ZuliaServiceGrpc.ZuliaServiceBlockingStub getService() {
		return blockingStub;
	}

	public ZuliaServiceGrpc.ZuliaServiceStub getAsyncService() {
		return asyncStub;
	}

	public Node getNode() {
		return node;
	}

	public boolean isCompressedConnection() {
		return compressedConnection;
	}

	/**
	 * closes the connection to the server
	 */
	public void close() {

		try {
			if (channel != null) {
				channel.shutdown();
				try {
					channel.awaitTermination(15, TimeUnit.SECONDS);
				}
				catch (InterruptedException ignored) {

				}
			}
		}
		finally {
			channel = null;
			blockingStub = null;
			asyncStub = null;
		}

	}

}
