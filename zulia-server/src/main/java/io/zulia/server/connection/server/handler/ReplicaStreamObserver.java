package io.zulia.server.connection.server.handler;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import io.zulia.message.ZuliaServiceOuterClass.SegmentFileData;
import io.zulia.message.ZuliaServiceOuterClass.SendSegmentFilesResponse;
import io.zulia.server.index.ZuliaIndexManager;
import io.zulia.server.index.replication.ReplicaDirectoryApplier;
import io.zulia.server.index.replication.ReplicationLimits;
import io.zulia.server.index.resident.IndexLease;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Semaphore;

public class ReplicaStreamObserver implements StreamObserver<SegmentFileData> {

	private final static Logger LOG = LoggerFactory.getLogger(ReplicaStreamObserver.class);

	private final ZuliaIndexManager indexManager;
	private final StreamObserver<SendSegmentFilesResponse> responseObserver;
	private final Semaphore inboundSemaphore;

	private IndexLease lease;
	private Semaphore applyLock;
	private ReplicaDirectoryApplier applier;
	private long generation;
	private boolean terminated;
	private boolean streamResourcesReleased;

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
				// the lease spans the whole inbound stream so the index cannot be unloaded while segment files are applied
				lease = indexManager.leaseIndexFromName(data.getIndexName());
				// serialize applies per index and shard: without this, a stream from a primary evicted
				// mid-push can overlap the reloaded primary's next push, and interleaved writes or
				// cleanup tear the replica directory (a published segments_N referencing deleted files)
				Semaphore shardApplyLock = indexManager.getReplicaApplyLock(data.getIndexName(), data.getShardNumber());
				shardApplyLock.acquire();
				applyLock = shardApplyLock;
				applier = lease.getIndex().newReplicaApplier(data.getShardNumber());
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
		releaseStreamResources();
	}

	@Override
	public void onCompleted() {
		if (terminated) {
			releaseStreamResources();
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
			releaseStreamResources();
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
			releaseStreamResources();
		}
	}

	private void releaseStreamResources() {
		if (!streamResourcesReleased) {
			streamResourcesReleased = true;
			if (applyLock != null) {
				applyLock.release();
				applyLock = null;
			}
			inboundSemaphore.release();
			if (lease != null) {
				lease.close();
			}
		}
	}
}
