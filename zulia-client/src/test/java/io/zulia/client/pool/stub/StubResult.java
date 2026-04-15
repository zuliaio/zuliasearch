package io.zulia.client.pool.stub;

import io.zulia.client.result.Result;

public class StubResult extends Result {
	private final String nodeAddress;

	public StubResult(String nodeAddress) {
		this.nodeAddress = nodeAddress;
	}

	public String getNodeAddress() {
		return nodeAddress;
	}
}
