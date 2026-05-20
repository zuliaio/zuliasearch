package io.zulia.server.connection.server.handler;

import io.grpc.stub.StreamObserver;
import io.zulia.message.ZuliaServiceOuterClass.SegmentFileData;

public class NoOpStreamObserver implements StreamObserver<SegmentFileData> {

	@Override
	public void onNext(SegmentFileData value) {
	}

	@Override
	public void onError(Throwable t) {
	}

	@Override
	public void onCompleted() {
	}
}
