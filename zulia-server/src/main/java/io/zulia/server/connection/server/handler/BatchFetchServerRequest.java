package io.zulia.server.connection.server.handler;

import io.grpc.stub.StreamObserver;
import io.zulia.message.ZuliaServiceOuterClass.BatchFetchRequest;
import io.zulia.message.ZuliaServiceOuterClass.FetchRequest;
import io.zulia.message.ZuliaServiceOuterClass.FetchResponse;
import io.zulia.server.index.ZuliaIndexManager;

import java.util.logging.Level;
import java.util.logging.Logger;

public class BatchFetchServerRequest {

	private final static Logger LOG = Logger.getLogger(BatchFetchServerRequest.class.getSimpleName());
	private final ZuliaIndexManager indexManager;

	public BatchFetchServerRequest(ZuliaIndexManager indexManager) {
		this.indexManager = indexManager;
	}

	public void handleRequest(BatchFetchRequest request, StreamObserver<FetchResponse> responseObserver) {
		try {
			for (FetchRequest fetchRequest : request.getFetchRequestList()) {
				FetchResponse fetchResponse = indexManager.fetch(fetchRequest);
				responseObserver.onNext(fetchResponse);
			}

			responseObserver.onCompleted();
		}
		catch (Exception e) {
			responseObserver.onError(e);
			onError(e);
		}
	}
	
	protected void onError(Exception e) {
		LOG.log(Level.SEVERE, "Failed to handle batch fetch", e);
	}
}
