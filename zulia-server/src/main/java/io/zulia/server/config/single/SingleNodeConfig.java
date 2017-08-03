package io.zulia.server.config.single;

import io.zulia.message.ZuliaBase.Node;
import io.zulia.server.config.NodeConfig;
import io.zulia.server.config.ZuliaConfig;

import java.util.Collections;
import java.util.List;

public class SingleNodeConfig implements NodeConfig {

	private final Node node;

	public SingleNodeConfig(ZuliaConfig zuliaConfig) {
		node = Node.newBuilder().setServerAddress(zuliaConfig.getServerAddress()).setHazelcastPort(zuliaConfig.getHazelcastPort())
				.setServicePort(zuliaConfig.getServicePort()).setRestPort(zuliaConfig.getRestPort()).build();
	}

	@Override
	public List<Node> getNodes() {

		return Collections.singletonList(node);
	}

	@Override
	public Node getNode(String serverAddress, int hazelcastPort) {
		if (node.getServerAddress().equals(serverAddress) && node.getHazelcastPort() == hazelcastPort) {
			return node;
		}
		return null;
	}

	@Override
	public void addNode(Node node) {
		throw new UnsupportedOperationException("Add node is only available in cluster mode");
	}

	@Override
	public void removeNode(String serverAddress, int hazelcastPort) {
		throw new UnsupportedOperationException("Remove node is only available in cluster mode");
	}
}
