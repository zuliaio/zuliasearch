package io.zulia.server.connection.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.zulia.message.ZuliaServiceGrpc;
import io.zulia.message.ZuliaServiceGrpc.ZuliaServiceBlockingStub;

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class InternalRpcConnection {

	private final static Logger log = Logger.getLogger(InternalRpcConnection.class.getSimpleName());
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

		log.info("Connecting to <" + memberAddress + ":" + servicePort + ">");
	}

	public ZuliaServiceBlockingStub getService() {
		return blockingStub;
	}

	public void close() {
		try {
			if (channel != null) {
				log.info("Closing connection to <" + memberAddress + ":" + internalServicePort + ">");
				channel.shutdown().awaitTermination(15, TimeUnit.SECONDS);
			}
		}
		catch (Exception e) {
			log.log(Level.SEVERE, "Close Failed", e);
		}
		channel = null;
		blockingStub = null;

	}
}
