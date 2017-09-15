package io.zulia.server.connection.server.handler;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import io.zulia.cache.MetaKeys;
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
		catch (IllegalArgumentException e) {

			Metadata metadata = new Metadata();
			metadata.put(MetaKeys.ERROR_KEY, e.getMessage());

			responseObserver.onError(new StatusException(Status.INVALID_ARGUMENT, metadata));
		}
		catch (Exception e) {
			responseObserver.onError(e);
			onError(e);
		}
	}

	protected abstract S handleCall(ZuliaIndexManager indexManager, Q request) throws Exception;

	protected abstract void onError(Exception e);

}
