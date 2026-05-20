package io.zulia.server.index.replication;

import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.zulia.message.ZuliaServiceOuterClass.SegmentFileData;
import io.zulia.message.ZuliaServiceOuterClass.SendSegmentFilesResponse;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class SendResponseObserver implements ClientResponseObserver<SegmentFileData, SendSegmentFilesResponse> {

	private final CompletableFuture<SendSegmentFilesResponse> responseFuture = new CompletableFuture<>();
	private final Object readyLock = new Object();

	@Override
	public void beforeStart(ClientCallStreamObserver<SegmentFileData> requestStream) {
		requestStream.setOnReadyHandler(() -> {
			synchronized (readyLock) {
				readyLock.notifyAll();
			}
		});
	}

	@Override
	public void onNext(SendSegmentFilesResponse response) {
		responseFuture.complete(response);
	}

	@Override
	public void onError(Throwable t) {
		responseFuture.completeExceptionally(t);
	}

	@Override
	public void onCompleted() {
		if (!responseFuture.isDone()) {
			responseFuture.completeExceptionally(new ReplicationStreamException("Stream completed without response"));
		}
	}

	void awaitReady(ClientCallStreamObserver<?> observer) throws InterruptedException {
		while (!observer.isReady()) {
			if (responseFuture.isDone()) {
				return;
			}
			synchronized (readyLock) {
				if (observer.isReady() || responseFuture.isDone()) {
					return;
				}
				readyLock.wait(100);
			}
		}
	}

	SendSegmentFilesResponse await(long timeout, TimeUnit unit) throws ExecutionException, InterruptedException, TimeoutException {
		return responseFuture.get(timeout, unit);
	}
}
