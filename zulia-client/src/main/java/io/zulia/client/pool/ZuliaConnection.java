package io.zulia.client.pool;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.zulia.message.ZuliaBase.Node;
import io.zulia.message.ZuliaServiceGrpc;

import java.util.logging.Level;
import java.util.logging.Logger;

public class ZuliaConnection {

	private static final Logger LOG = Logger.getLogger(ZuliaConnection.class.getName());
	private final long connectionId;
	private final long connectionNumberForNode;
	private final boolean compressedConnection;

	private final Node node;

	private ManagedChannel channel;

	private ZuliaServiceGrpc.ZuliaServiceBlockingStub blockingStub;
	private ZuliaServiceGrpc.ZuliaServiceStub asyncStub;

	public ZuliaConnection(Node node, boolean compressedConnection, long connectionId, long connectionNumberForNode) {
		this.node = node;
		this.connectionId = connectionId;
		this.connectionNumberForNode = connectionNumberForNode;
		this.compressedConnection = compressedConnection;
	}

	public void open() {

		ManagedChannelBuilder<?> managedChannelBuilder = ManagedChannelBuilder.forAddress(node.getServerAddress(), node.getServicePort())
				.maxInboundMessageSize(256 * 1024 * 1024).usePlaintext();
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

	public long getConnectionId() {
		return connectionId;
	}

	public long getConnectionNumberForNode() {
		return connectionNumberForNode;
	}

	/**
	 * closes the connection to the server if open, calling a method (index, query, ...) will open a new connection
	 */
	public void close() {

		try {
			if (channel != null) {
				channel.shutdownNow();
			}
		}
		catch (Exception e) {
			LOG.log(Level.SEVERE, "Exception closing: ", e);
		}
		channel = null;
		blockingStub = null;
		asyncStub = null;

	}

}
