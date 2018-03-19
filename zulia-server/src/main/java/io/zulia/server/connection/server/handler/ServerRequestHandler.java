package io.zulia.server.connection.server.handler;

import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusException;
import io.grpc.stub.StreamObserver;
import io.zulia.cache.MetaKeys;
import io.zulia.server.exceptions.ShardOfflineException;
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
		catch (Throwable e) {
			Metadata metadata = new Metadata();
			if (e.getMessage() != null) {
				metadata.put(MetaKeys.ERROR_KEY, e.getMessage());
			}
			else {
				metadata.put(MetaKeys.ERROR_KEY, e.getClass().getSimpleName());
			}
			Status status = Status.UNKNOWN;
			if (e instanceof IllegalArgumentException) {
				status = Status.INVALID_ARGUMENT;
			}
			if (e instanceof ShardOfflineException) {
				status = Status.UNAVAILABLE;
			}

			responseObserver.onError(new StatusException(status, metadata));
			onError(e);
		}
	}

	protected abstract S handleCall(ZuliaIndexManager indexManager, Q request) throws Exception;

	protected abstract void onError(Throwable e);

}
