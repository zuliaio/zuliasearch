package io.zulia.client.pool.stub;

import io.zulia.client.command.base.ShardRoutableCommand;

public class StubShardCommand extends StubGrpcCommand implements ShardRoutableCommand {
	private final String indexName;
	private final String uniqueId;

	public StubShardCommand(String indexName, String uniqueId) {
		this.indexName = indexName;
		this.uniqueId = uniqueId;
	}

	@Override
	public String getUniqueId() {
		return uniqueId;
	}

	@Override
	public String getIndexName() {
		return indexName;
	}
}
