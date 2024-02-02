package io.zulia.server.connection.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.zulia.message.ZuliaServiceGrpc;
import io.zulia.message.ZuliaServiceGrpc.ZuliaServiceBlockingStub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class InternalRpcConnection {

	private final static Logger LOG = LoggerFactory.getLogger(InternalRpcConnection.class);
	private final String memberAddress;
	private final int internalServicePort;

	private ManagedChannel channel;
	private ZuliaServiceBlockingStub blockingStub;

	public InternalRpcConnection(String memberAddress, int servicePort) {
		this.memberAddress = memberAddress;
		this.internalServicePort = servicePort;

		ManagedChannelBuilder<?> managedChannelBuilder = ManagedChannelBuilder.forAddress(memberAddress, servicePort).maxInboundMessageSize(256 * 1024 * 1024)
				.usePlaintext();
		channel = managedChannelBuilder.build();

		blockingStub = ZuliaServiceGrpc.newBlockingStub(channel);

		LOG.info("Connecting to <" + memberAddress + ":" + servicePort + ">");
	}

	public ZuliaServiceBlockingStub getService() {
		return blockingStub;
	}

	public void close() {
		try {
			if (channel != null) {
				LOG.info("Closing connection to <" + memberAddress + ":" + internalServicePort + ">");
				channel.shutdown();
				try {
					channel.awaitTermination(15, TimeUnit.SECONDS);
				}
				catch (InterruptedException ex) {
					LOG.warn("connection to <" + memberAddress + ":" + internalServicePort + "> timed out on close");
				}

			}
		}
		catch (Exception e) {
			LOG.error("Close Failed", e);
		}

		channel = null;
		blockingStub = null;

	}
}
