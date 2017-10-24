package io.zulia.client.pool;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.zulia.client.ZuliaRESTClient;
import io.zulia.message.ZuliaBase.Node;
import io.zulia.message.ZuliaServiceGrpc;

import java.io.IOException;

public class ZuliaConnection {

	private Node node;

	private ManagedChannel channel;

	private ZuliaServiceGrpc.ZuliaServiceBlockingStub blockingStub;
	private ZuliaServiceGrpc.ZuliaServiceStub asyncStub;

	public ZuliaConnection(Node node) throws IOException {
		this.node = node;
	}

	public void open(boolean compressedConnection) throws IOException {

		ManagedChannelBuilder<?> managedChannelBuilder = ManagedChannelBuilder.forAddress(node.getServerAddress(), node.getServicePort())
				.maxInboundMessageSize(128 * 1024 * 1024).usePlaintext(true);
		channel = managedChannelBuilder.build();

		blockingStub = ZuliaServiceGrpc.newBlockingStub(channel);
		if (compressedConnection) {
			blockingStub = blockingStub.withCompression("gzip");
		}

		asyncStub = ZuliaServiceGrpc.newStub(channel);
		if (compressedConnection) {
			asyncStub = asyncStub.withCompression("gzip");
		}

		System.err.println("INFO: Connecting to <" + node.getServerAddress() + ":" + node.getServicePort() + ">");

	}

	public ZuliaRESTClient getRestClient() throws Exception {
		return new ZuliaRESTClient(node.getServerAddress(), node.getRestPort());
	}

	public ZuliaServiceGrpc.ZuliaServiceBlockingStub getService() {
		return blockingStub;
	}

	public ZuliaServiceGrpc.ZuliaServiceStub getAsyncService() {
		return asyncStub;
	}

	/**
	 * closes the connection to the server if open, calling a method (index, query, ...) will open a new connection
	 */
	public void close() {
		System.err.println("INFO: Closing connection to <" + node.getServerAddress() + ":" + node.getServicePort() + ">");
		try {
			if (channel != null) {
				channel.shutdownNow();
			}
		}
		catch (Exception e) {
			System.err.println("ERROR: Exception: " + e);
			e.printStackTrace();
		}
		channel = null;
		blockingStub = null;
		asyncStub = null;

	}

}
