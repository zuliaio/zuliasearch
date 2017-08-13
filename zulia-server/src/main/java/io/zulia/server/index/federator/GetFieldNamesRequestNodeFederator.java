package io.zulia.server.index.federator;

import io.zulia.message.ZuliaBase;
import io.zulia.message.ZuliaBase.Node;
import io.zulia.message.ZuliaServiceOuterClass.GetFieldNamesRequest;
import io.zulia.message.ZuliaServiceOuterClass.GetFieldNamesResponse;
import io.zulia.server.connection.client.InternalClient;
import io.zulia.server.index.ZuliaIndex;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;

public class GetFieldNamesRequestNodeFederator extends RequestNodeFederator<GetFieldNamesRequest, GetFieldNamesResponse> {
	private final InternalClient internalClient;
	private final ZuliaIndex index;

	public GetFieldNamesRequestNodeFederator(Node thisNode, Collection<Node> otherNodesActive, ZuliaBase.MasterSlaveSettings masterSlaveSettings,
			ZuliaIndex index, ExecutorService pool, InternalClient internalClient) throws IOException {
		super(thisNode, otherNodesActive, masterSlaveSettings, index, pool);
		this.internalClient = internalClient;
		this.index = index;
	}

	@Override
	protected GetFieldNamesResponse processExternal(Node node, GetFieldNamesRequest request) throws Exception {
		return internalClient.getFieldNames(node, request);
	}

	@Override
	protected GetFieldNamesResponse processInternal(GetFieldNamesRequest request) throws Exception {
		return internalGetFieldNames(index, request);
	}

	public static GetFieldNamesResponse internalGetFieldNames(ZuliaIndex index, GetFieldNamesRequest request) throws Exception {
		return index.getFieldNames(request);
	}

	public GetFieldNamesResponse getResponse(GetFieldNamesRequest request) throws Exception {

		Set<String> fieldNames = new HashSet<>();
		List<GetFieldNamesResponse> responses = send(request);
		for (GetFieldNamesResponse response : responses) {
			fieldNames.addAll(response.getFieldNameList());
		}

		GetFieldNamesResponse.Builder responseBuilder = GetFieldNamesResponse.newBuilder();
		responseBuilder.addAllFieldName(fieldNames);
		return responseBuilder.build();
	}
}
