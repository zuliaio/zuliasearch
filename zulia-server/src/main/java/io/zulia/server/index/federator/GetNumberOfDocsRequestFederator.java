package io.zulia.server.index.federator;

import io.zulia.message.ZuliaBase;
import io.zulia.message.ZuliaBase.Node;
import io.zulia.message.ZuliaBase.ShardCountResponse;
import io.zulia.message.ZuliaServiceOuterClass.GetNumberOfDocsRequest;
import io.zulia.message.ZuliaServiceOuterClass.GetNumberOfDocsResponse;
import io.zulia.server.connection.client.InternalClient;
import io.zulia.server.index.ZuliaIndex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class GetNumberOfDocsRequestFederator extends MasterSlaveNodeRequestFederator<GetNumberOfDocsRequest, GetNumberOfDocsResponse> {
	private final InternalClient internalClient;
	private final ZuliaIndex index;

	public GetNumberOfDocsRequestFederator(Node thisNode, Collection<Node> otherNodesActive, ZuliaBase.MasterSlaveSettings masterSlaveSettings,
			ZuliaIndex index, ExecutorService pool, InternalClient internalClient) throws IOException {
		super(thisNode, otherNodesActive, masterSlaveSettings, index, pool);
		this.internalClient = internalClient;
		this.index = index;
	}

	@Override
	protected GetNumberOfDocsResponse processExternal(Node node, GetNumberOfDocsRequest request) throws Exception {
		return internalClient.getNumberOfDocs(node, request);
	}

	@Override
	protected GetNumberOfDocsResponse processInternal(GetNumberOfDocsRequest request) throws Exception {
		return internalGetNumberOfDocs(index, request);
	}

	public static GetNumberOfDocsResponse internalGetNumberOfDocs(ZuliaIndex index, GetNumberOfDocsRequest request) throws Exception {
		return index.getNumberOfDocs();
	}

	public GetNumberOfDocsResponse getResponse(GetNumberOfDocsRequest request) throws Exception {

		List<GetNumberOfDocsResponse> responses = send(request);

		GetNumberOfDocsResponse.Builder responseBuilder = GetNumberOfDocsResponse.newBuilder();
		responseBuilder.setNumberOfDocs(0);

		List<ShardCountResponse> shardCountResponseList = new ArrayList<>();

		for (GetNumberOfDocsResponse r : responses) {
			responseBuilder.setNumberOfDocs(responseBuilder.getNumberOfDocs() + r.getNumberOfDocs());
			shardCountResponseList.addAll(r.getShardCountResponseList());
		}

		shardCountResponseList.sort(Comparator.comparingInt(ShardCountResponse::getShardNumber));
		responseBuilder.addAllShardCountResponse(shardCountResponseList);

		return responseBuilder.build();

	}
}
