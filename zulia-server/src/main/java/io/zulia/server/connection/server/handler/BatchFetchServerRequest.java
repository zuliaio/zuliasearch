package io.zulia.server.connection.server.handler;

import io.grpc.stub.StreamObserver;
import io.zulia.message.ZuliaServiceOuterClass.BatchFetchRequest;
import io.zulia.message.ZuliaServiceOuterClass.FetchResponse;
import io.zulia.server.index.ZuliaIndexManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchFetchServerRequest {

	private final static Logger LOG = LoggerFactory.getLogger(BatchFetchServerRequest.class);
	private final ZuliaIndexManager indexManager;

	public BatchFetchServerRequest(ZuliaIndexManager indexManager) {
		this.indexManager = indexManager;
	}

	public void handleRequest(BatchFetchRequest request, StreamObserver<FetchResponse> responseObserver) {
		try {
			indexManager.batchFetch(request, responseObserver);
			responseObserver.onCompleted();
		}
		catch (Exception e) {
			responseObserver.onError(e);
			onError(e);
		}
	}

	protected void onError(Exception e) {
		LOG.error("Failed to handle batch fetch", e);
	}
}
