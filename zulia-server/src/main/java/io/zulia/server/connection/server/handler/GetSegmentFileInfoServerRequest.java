package io.zulia.server.connection.server.handler;

import io.zulia.message.ZuliaServiceOuterClass.GetSegmentFileInfoRequest;
import io.zulia.message.ZuliaServiceOuterClass.GetSegmentFileInfoResponse;
import io.zulia.message.ZuliaServiceOuterClass.SegmentFileInfo;
import io.zulia.server.index.ZuliaIndex;
import io.zulia.server.index.ZuliaIndexManager;
import io.zulia.server.index.replication.ReplicaFileInfo;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class GetSegmentFileInfoServerRequest extends ServerRequestHandler<GetSegmentFileInfoResponse, GetSegmentFileInfoRequest> {

	private final static Logger LOG = LoggerFactory.getLogger(GetSegmentFileInfoServerRequest.class);

	public GetSegmentFileInfoServerRequest(ZuliaIndexManager indexManager) {
		super(indexManager);
	}

	@Override
	protected GetSegmentFileInfoResponse handleCall(ZuliaIndexManager indexManager, GetSegmentFileInfoRequest request) throws Exception {
		ZuliaIndex zuliaIndex = indexManager.getIndexFromName(request.getIndexName());
		List<ReplicaFileInfo> files = zuliaIndex.listReplicaFileInfo(request.getShardNumber(), request.getTaxonomy());
		GetSegmentFileInfoResponse.Builder responseBuilder = GetSegmentFileInfoResponse.newBuilder().setLuceneVersion(Version.LATEST.toString());
		for (ReplicaFileInfo info : files) {
			responseBuilder.addFiles(SegmentFileInfo.newBuilder().setFileName(info.name()).setLength(info.length()).setChecksum(info.checksum()).build());
		}
		return responseBuilder.build();
	}

	@Override
	protected void onError(Throwable e) {
		LOG.error("Failed to handle get segment file info", e);
	}
}
