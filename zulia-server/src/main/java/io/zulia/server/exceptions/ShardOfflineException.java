package io.zulia.server.exceptions;

import io.zulia.message.ZuliaBase.PrimaryReplicaSettings;

public class ShardOfflineException extends NotFoundException {

	private static final long serialVersionUID = 1L;
	private final int shardNumber;
	private final PrimaryReplicaSettings primaryReplicaSettings;
	private final String indexName;

	public ShardOfflineException(String indexName, int shardNumber, PrimaryReplicaSettings primaryReplicaSettings) {
		super("Index <" + indexName + "> with shardNumber <" + shardNumber + "> is offline for <" + primaryReplicaSettings + ">");
		this.indexName = indexName;
		this.shardNumber = shardNumber;
		this.primaryReplicaSettings = primaryReplicaSettings;
	}

	public String getIndexName() {
		return indexName;
	}

	public int getShardNumber() {
		return shardNumber;
	}

	public PrimaryReplicaSettings getPrimaryReplicaSettings() {
		return primaryReplicaSettings;
	}
}