package io.zulia.server.connection.client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.zulia.message.ZuliaServiceGrpc;
import io.zulia.message.ZuliaServiceGrpc.ZuliaServiceBlockingStub;
import io.zulia.message.ZuliaServiceGrpc.ZuliaServiceStub;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class InternalRpcConnection {

	private final static Logger LOG = LoggerFactory.getLogger(InternalRpcConnection.class);
	private final String memberAddress;
	private final int internalServicePort;

	// final so a concurrent close() can never surface a null stub to an in-flight request. Calls on a
	// shutdown channel fail with a clean retriable StatusRuntimeException instead of an NPE.
	private final ManagedChannel channel;
	private final ZuliaServiceBlockingStub blockingStub;
	private final ZuliaServiceStub asyncStub;

	public InternalRpcConnection(String memberAddress, int servicePort) {
		this.memberAddress = memberAddress;
		this.internalServicePort = servicePort;

		ManagedChannelBuilder<?> managedChannelBuilder = ManagedChannelBuilder.forAddress(memberAddress, servicePort).maxInboundMessageSize(256 * 1024 * 1024)
				.usePlaintext().keepAliveTime(30, TimeUnit.SECONDS).keepAliveTimeout(5, TimeUnit.SECONDS).keepAliveWithoutCalls(true);
		channel = managedChannelBuilder.build();

		blockingStub = ZuliaServiceGrpc.newBlockingStub(channel);
		asyncStub = ZuliaServiceGrpc.newStub(channel);

		LOG.info("Connecting to {}:{}", memberAddress, servicePort);
	}

	public ZuliaServiceBlockingStub getService() {
		return blockingStub;
	}

	public ZuliaServiceStub getAsyncService() {
		return asyncStub;
	}

	public void close() {
		try {
			LOG.info("Closing connection to {}:{}", memberAddress, internalServicePort);
			channel.shutdown();
			try {
				if (!channel.awaitTermination(15, TimeUnit.SECONDS)) {
					LOG.warn("Connection to {}:{} did not terminate within 15s on close, forcing shutdown", memberAddress, internalServicePort);
					channel.shutdownNow();
				}
			}
			catch (InterruptedException ex) {
				LOG.warn("Interrupted while closing connection to {}:{}, forcing shutdown", memberAddress, internalServicePort);
				channel.shutdownNow();
				Thread.currentThread().interrupt();
			}
		}
		catch (Exception e) {
			LOG.error("Close failed: ", e);
		}
	}
}
