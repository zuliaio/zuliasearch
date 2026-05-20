package io.zulia.server.index.replication;

import io.zulia.message.ZuliaBase.Node;

public record ReplicaShardKey(String serverAddress, int servicePort, int shardNumber) {

	public static ReplicaShardKey of(Node replicaNode, int shardNumber) {
		return new ReplicaShardKey(replicaNode.getServerAddress(), replicaNode.getServicePort(), shardNumber);
	}
}
