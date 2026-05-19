package io.zulia.server.connection.server.handler;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import io.zulia.message.ZuliaServiceOuterClass.SegmentFileData;
import io.zulia.message.ZuliaServiceOuterClass.SendSegmentFilesResponse;
import io.zulia.server.index.ZuliaIndexManager;
import io.zulia.server.index.replication.ReplicaDirectoryApplier;
import io.zulia.server.index.replication.ReplicationLimits;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Semaphore;

public class ReplicaStreamObserver implements StreamObserver<SegmentFileData> {

	private final static Logger LOG = LoggerFactory.getLogger(ReplicaStreamObserver.class);

	private final ZuliaIndexManager indexManager;
	private final StreamObserver<SendSegmentFilesResponse> responseObserver;
	private final Semaphore inboundSemaphore;

	private ReplicaDirectoryApplier applier;
	private long generation;
	private boolean terminated;
	private boolean permitReleased;

	public ReplicaStreamObserver(ZuliaIndexManager indexManager, StreamObserver<SendSegmentFilesResponse> responseObserver, Semaphore inboundSemaphore) {
		this.indexManager = indexManager;
		this.responseObserver = responseObserver;
		this.inboundSemaphore = inboundSemaphore;
	}

	@Override
	public void onNext(SegmentFileData data) {
		if (terminated) {
			return;
		}
		try {
			if (applier == null) {
				applier = indexManager.getIndexFromName(data.getIndexName()).newReplicaApplier(data.getShardNumber());
			}

			generation = data.getGeneration();

			ByteString payload = data.getData();
			if (payload.size() > ReplicationLimits.MAX_CHUNK_BYTES) {
				throw new IllegalArgumentException("Chunk size " + payload.size() + " exceeds cap " + ReplicationLimits.MAX_CHUNK_BYTES);
			}
			byte[] buf = payload.toByteArray();
			applier.writeBytes(data.getTaxonomy(), data.getFileName(), buf, 0, buf.length);

			if (data.getLastChunk()) {
				applier.finishFile();
			}
		}
		catch (Exception e) {
			terminate(e);
		}
	}

	@Override
	public void onError(Throwable t) {
		// Client cancelled or dropped; the call is already terminated, do not call responseObserver.onError.
		LOG.warn("Error in send segment files stream: {}", t.getMessage());
		terminated = true;
		if (applier != null) {
			applier.abort();
		}
		releasePermit();
	}

	@Override
	public void onCompleted() {
		if (terminated) {
			releasePermit();
			return;
		}
		try {
			if (applier != null) {
				applier.commit();
			}
			responseObserver.onNext(SendSegmentFilesResponse.newBuilder().setSuccess(true).setGeneration(generation).build());
			responseObserver.onCompleted();
		}
		catch (Exception e) {
			// Do not ack: the primary advances the generation gate only on success and will retry next commit.
			LOG.error("Commit on replica failed during onCompleted for generation {}", generation, e);
			terminated = true;
			if (applier != null) {
				applier.abort();
			}
			try {
				responseObserver.onError(e);
			}
			catch (IllegalStateException ignored) {
			}
		}
		finally {
			releasePermit();
		}
	}

	private void terminate(Exception e) {
		LOG.error("Error receiving segment file data", e);
		if (!terminated) {
			terminated = true;
			if (applier != null) {
				applier.abort();
			}
			try {
				responseObserver.onError(e);
			}
			catch (IllegalStateException ignored) {
			}
			releasePermit();
		}
	}

	private void releasePermit() {
		if (!permitReleased) {
			permitReleased = true;
			inboundSemaphore.release();
		}
	}
}
