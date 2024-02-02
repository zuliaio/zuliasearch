package io.zulia.server.connection.server.handler;

import io.grpc.stub.StreamObserver;
import io.zulia.message.ZuliaServiceOuterClass.BatchDeleteRequest;
import io.zulia.message.ZuliaServiceOuterClass.DeleteRequest;
import io.zulia.message.ZuliaServiceOuterClass.DeleteResponse;
import io.zulia.server.index.ZuliaIndexManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchDeleteServerRequest {

	private final static Logger LOG = LoggerFactory.getLogger(BatchDeleteServerRequest.class);
	private final ZuliaIndexManager indexManager;

	public BatchDeleteServerRequest(ZuliaIndexManager indexManager) {
		this.indexManager = indexManager;
	}

	public void handleRequest(BatchDeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
		try {
			for (DeleteRequest deleteRequest : request.getRequestList()) {
				DeleteResponse deleteResponse = indexManager.delete(deleteRequest);
				responseObserver.onNext(deleteResponse);
			}

			responseObserver.onCompleted();
		}
		catch (Exception e) {
			responseObserver.onError(e);
			onError(e);
		}
	}

	protected void onError(Exception e) {
		LOG.error("Failed to handle batch delete", e);
	}
}
