package io.zulia.server.index.replication;

import com.google.protobuf.UnsafeByteOperations;
import io.grpc.stub.ClientCallStreamObserver;
import io.zulia.message.ZuliaBase.Node;
import io.zulia.message.ZuliaServiceOuterClass.GetSegmentFileInfoRequest;
import io.zulia.message.ZuliaServiceOuterClass.GetSegmentFileInfoResponse;
import io.zulia.message.ZuliaServiceOuterClass.SegmentFileData;
import io.zulia.message.ZuliaServiceOuterClass.SegmentFileInfo;
import io.zulia.message.ZuliaServiceOuterClass.SendSegmentFilesResponse;
import io.zulia.server.connection.client.InternalClient;
import io.zulia.server.connection.client.InternalRpcConnection;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class DirectSegmentReplicator {

	private static final long INFO_TIMEOUT_SECONDS = 30;

	private static final String COMPRESSION = "gzip";

	private final InternalClient internalClient;
	private final Node replicaNode;
	private final long responseTimeoutMinutes;
	private final ReplicationRateLimiter rateLimiter;

	public DirectSegmentReplicator(InternalClient internalClient, Node replicaNode, long responseTimeoutMinutes, ReplicationRateLimiter rateLimiter) {
		this.internalClient = internalClient;
		this.replicaNode = replicaNode;
		this.responseTimeoutMinutes = responseTimeoutMinutes;
		this.rateLimiter = rateLimiter;
	}

	public GetSegmentFileInfoResponse getSegmentFileInfo(String indexName, int shardNumber, boolean taxonomy) throws Exception {
		InternalRpcConnection rpcConnection = internalClient.getConnection(replicaNode);
		GetSegmentFileInfoRequest request = GetSegmentFileInfoRequest.newBuilder().setIndexName(indexName).setShardNumber(shardNumber).setTaxonomy(taxonomy)
				.build();
		// Bounded deadline so a hung replica can't hold the primary's shard lock indefinitely.
		return rpcConnection.getService().withCompression(COMPRESSION).withDeadlineAfter(INFO_TIMEOUT_SECONDS, TimeUnit.SECONDS)
				.internalGetSegmentFileInfo(request);
	}

	public SendSegmentFilesResponse sendSegmentFiles(String indexName, int shardNumber, boolean taxonomy, List<SegmentFileInfo> filesToSend,
			Directory sourceDirectory, long generation) throws Exception {

		InternalRpcConnection rpcConnection = internalClient.getConnection(replicaNode);

		SendResponseObserver responseObserver = new SendResponseObserver();

		// Deadline on the call (not just responseFuture.get) so expiry cancels the stream and completes exceptionally.
		ClientCallStreamObserver<SegmentFileData> requestObserver = (ClientCallStreamObserver<SegmentFileData>) rpcConnection.getAsyncService()
				.withCompression(COMPRESSION).withDeadlineAfter(responseTimeoutMinutes, TimeUnit.MINUTES).internalSendSegmentFiles(responseObserver);

		try {
			for (SegmentFileInfo fileInfo : filesToSend) {
				String fileName = fileInfo.getFileName();
				try (IndexInput input = sourceDirectory.openInput(fileName, IOContext.READONCE)) {
					long remaining = input.length();

					while (remaining > 0) {
						int toRead = (int) Math.min(ReplicationLimits.CHUNK_SIZE, remaining);
						// Acquire byte permits before the disk read so a stalled rate limiter doesn't pin a buffer.
						rateLimiter.acquire(toRead);
						// Fresh byte[] each chunk: unsafeWrap takes ownership, so the buffer must not be mutated after.
						byte[] buffer = new byte[toRead];
						input.readBytes(buffer, 0, toRead);
						remaining -= toRead;

						responseObserver.awaitReady(requestObserver);

						SegmentFileData chunk = SegmentFileData.newBuilder().setIndexName(indexName).setShardNumber(shardNumber).setTaxonomy(taxonomy)
								.setFileName(fileName).setData(UnsafeByteOperations.unsafeWrap(buffer)).setGeneration(generation)
								.setLastChunk(remaining == 0).build();

						requestObserver.onNext(chunk);
					}
				}
			}
			requestObserver.onCompleted();
		}
		catch (Exception e) {
			try {
				requestObserver.onError(e);
			}
			catch (Exception suppressed) {
				e.addSuppressed(suppressed);
			}
			throw e;
		}

		return responseObserver.await(responseTimeoutMinutes, TimeUnit.MINUTES);
	}
}
