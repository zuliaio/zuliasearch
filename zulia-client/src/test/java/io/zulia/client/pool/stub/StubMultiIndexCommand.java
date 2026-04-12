package io.zulia.client.pool.stub;

import io.zulia.client.command.base.MultiIndexRoutableCommand;

import java.util.Collection;
import java.util.List;

public class StubMultiIndexCommand extends StubGrpcCommand implements MultiIndexRoutableCommand {
	private final List<String> indexNames;

	public StubMultiIndexCommand(List<String> indexNames) {
		this.indexNames = indexNames;
	}

	@Override
	public Collection<String> getIndexNames() {
		return indexNames;
	}
}
