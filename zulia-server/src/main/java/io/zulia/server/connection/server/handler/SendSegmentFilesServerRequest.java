package io.zulia.server.connection.server.handler;

import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import io.zulia.message.ZuliaServiceOuterClass.SegmentFileData;
import io.zulia.message.ZuliaServiceOuterClass.SendSegmentFilesResponse;
import io.zulia.server.index.ZuliaIndexManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Semaphore;

public class SendSegmentFilesServerRequest {

	private final static Logger LOG = LoggerFactory.getLogger(SendSegmentFilesServerRequest.class);

	// Excess streams fail with RESOURCE_EXHAUSTED so the primary retries next commit rather than queueing.
	private static final int MAX_CONCURRENT_INBOUND = 16;

	private final ZuliaIndexManager indexManager;
	private final Semaphore inboundSemaphore = new Semaphore(MAX_CONCURRENT_INBOUND);

	public SendSegmentFilesServerRequest(ZuliaIndexManager indexManager) {
		this.indexManager = indexManager;
	}

	public StreamObserver<SegmentFileData> handleRequest(StreamObserver<SendSegmentFilesResponse> responseObserver) {
		if (!inboundSemaphore.tryAcquire()) {
			LOG.warn("Rejecting inbound replication: {} streams already in progress", MAX_CONCURRENT_INBOUND);
			responseObserver.onError(Status.RESOURCE_EXHAUSTED.withDescription("Too many concurrent replications").asRuntimeException());
			return new NoOpStreamObserver();
		}
		return new ReplicaStreamObserver(indexManager, responseObserver, inboundSemaphore);
	}
}
