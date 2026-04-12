package io.zulia.client.pool.stub;

import io.zulia.client.command.base.SingleIndexRoutableCommand;

public class StubSingleIndexCommand extends StubGrpcCommand implements SingleIndexRoutableCommand {
	private final String indexName;

	public StubSingleIndexCommand(String indexName) {
		this.indexName = indexName;
	}

	@Override
	public String getIndexName() {
		return indexName;
	}
}
