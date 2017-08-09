package io.zulia.server.connection.server.handler;

import io.grpc.stub.StreamObserver;
import io.zulia.server.index.ZuliaIndexManager;

public abstract class ServerRequestHandler<S, Q> {

	private final ZuliaIndexManager indexManager;

	public ServerRequestHandler(ZuliaIndexManager indexManager) {
		this.indexManager = indexManager;
	}

	public void handleRequest(Q request, StreamObserver<S> responseObserver) {
		try {
			S s = handleCall(indexManager, request);
			responseObserver.onNext(s);
			responseObserver.onCompleted();
		}
		catch (Exception e) {
			responseObserver.onError(e);
			onError(e);
		}
	}

	protected abstract S handleCall(ZuliaIndexManager indexManager, Q request);

	protected abstract void onError(Exception e);

}
