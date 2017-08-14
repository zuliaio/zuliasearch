package io.zulia.server.index.router;

import io.zulia.message.ZuliaBase.MasterSlaveSettings;
import io.zulia.message.ZuliaBase.Node;
import io.zulia.message.ZuliaIndex.IndexMapping;
import io.zulia.server.exceptions.ShardDoesNotExistException;
import io.zulia.server.index.RequestNodeBase;
import io.zulia.server.index.ZuliaIndex;
import io.zulia.util.ShardUtil;

import java.io.IOException;
import java.util.Collection;

public abstract class RequestNodeRouter<I, O> extends RequestNodeBase<I, O> {

	private Node node;

	public RequestNodeRouter(Node thisNode, Collection<Node> otherNodesActive, MasterSlaveSettings masterSlaveSettings, ZuliaIndex index, String uniqueId)
			throws IOException {

		super(thisNode, otherNodesActive, masterSlaveSettings);

		IndexMapping indexMapping = index.getIndexMapping();
		int shardForUniqueId = ShardUtil.findShardForUniqueId(uniqueId, indexMapping.getNumberOfShards());

		for (io.zulia.message.ZuliaIndex.ShardMapping shardMapping : indexMapping.getShardMappingList()) {
			if (shardMapping.getShardNumber() == shardForUniqueId) {
				node = getNodeFromShardMapping(index.getIndexName(), shardMapping);
				break;
			}
		}
		if (node == null) {
			throw new ShardDoesNotExistException(index.getIndexName(), shardForUniqueId);
		}

	}

	public O send(final I request) throws Exception {
		if (nodeIsLocal(node)) {
			return processInternal(request);
		}
		return processExternal(node, request);
	}

}
