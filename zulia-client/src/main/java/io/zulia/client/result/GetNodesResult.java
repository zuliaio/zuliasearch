package io.zulia.client.result;

import io.zulia.message.ZuliaIndex.IndexAlias;
import io.zulia.message.ZuliaIndex.IndexShardMapping;

import java.util.List;

import static io.zulia.message.ZuliaBase.Node;
import static io.zulia.message.ZuliaServiceOuterClass.GetNodesResponse;

public class GetNodesResult extends Result {

	private GetNodesResponse getNodesResponse;

	public GetNodesResult(GetNodesResponse getNodesResponse) {
		this.getNodesResponse = getNodesResponse;
	}

	public List<Node> getNodes() {
		return getNodesResponse.getNodeList();
	}

	public List<IndexShardMapping> getIndexMappings() {
		return getNodesResponse.getIndexMappingList();
	}

	public List<IndexAlias> getIndexAliases() {
		return getNodesResponse.getIndexAliasList();
	}

	@Override
	public String toString() {
		return getNodesResponse.toString();
	}

}
