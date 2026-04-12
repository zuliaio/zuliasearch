package io.zulia.client.pool.stub;

import io.zulia.client.command.base.GrpcCommand;
import io.zulia.client.pool.ZuliaConnection;

public class StubGrpcCommand extends GrpcCommand<StubResult> {
	private final RuntimeException errorToThrow;

	public StubGrpcCommand() {
		this.errorToThrow = null;
	}

	public StubGrpcCommand(RuntimeException errorToThrow) {
		this.errorToThrow = errorToThrow;
	}

	@Override
	public StubResult execute(ZuliaConnection zuliaConnection) {
		if (errorToThrow != null) {
			throw errorToThrow;
		}
		return new StubResult(zuliaConnection.getNode().getServerAddress());
	}
}
