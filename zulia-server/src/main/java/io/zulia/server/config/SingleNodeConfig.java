package io.zulia.server.config;

import io.zulia.message.ZuliaBase.Node;

import java.util.Collections;
import java.util.List;

public class SingleNodeConfig implements NodeConfig {

	private final ZuliaConfig zuliaConfig;

	public SingleNodeConfig(ZuliaConfig zuliaConfig) {
		this.zuliaConfig = zuliaConfig;
	}

	@Override
	public List<Node> getNodes() {
		return Collections.singletonList(Node.newBuilder().setServerAddress(zuliaConfig.getServerAddress()).setHazelcastPort(zuliaConfig.getHazelcastPort())
				.setServicePort(zuliaConfig.getServicePort()).setRestPort(zuliaConfig.getRestPort()).build());
	}

	@Override
	public void addNode(Node node) {
		throw new IllegalArgumentException("Add node is only available in cluster mode");
	}

	@Override
	public void removeNode(Node node) {
		throw new IllegalArgumentException("Remove node is only available in cluster mode");
	}
}
