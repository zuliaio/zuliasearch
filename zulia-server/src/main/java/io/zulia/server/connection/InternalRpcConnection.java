package io.zulia.server.connection;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.zulia.message.ZuliaServiceGrpc;
import io.zulia.message.ZuliaServiceGrpc.ZuliaServiceBlockingStub;

import java.util.concurrent.TimeUnit;
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

		ManagedChannelBuilder<?> managedChannelBuilder = ManagedChannelBuilder.forAddress(memberAddress, servicePort).maxInboundMessageSize(128 * 1024 * 1024)
				.usePlaintext(true);
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
			System.err.println("ERROR: Exception: " + e);
			e.printStackTrace();
		}
		channel = null;
		blockingStub = null;

	}
}
