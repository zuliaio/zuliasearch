package io.zulia.server.config.single;

import io.zulia.message.ZuliaBase.Node;
import io.zulia.server.config.NodeService;
import io.zulia.server.config.ZuliaConfig;

import java.util.Collections;
import java.util.List;

public class SingleNodeService implements NodeService {

	private Node node;

	public SingleNodeService(ZuliaConfig zuliaConfig) {
		node = Node.newBuilder().setServerAddress(zuliaConfig.getServerAddress())
				.setServicePort(zuliaConfig.getServicePort()).setRestPort(zuliaConfig.getRestPort()).build();
	}

	@Override
	public List<Node> getNodes() {

		return Collections.singletonList(node);
	}

	@Override
	public Node getNode(String serverAddress, int servicePort) {
		if (node.getServerAddress().equals(serverAddress) && node.getServicePort() == servicePort) {
			return node;
		}
		return null;
	}

	@Override
	public void updateHeartbeat(String serverAddress, int servicePort) {
		node = node.toBuilder().setHeartbeat(System.currentTimeMillis()).build();
	}

	@Override
	public void removeHeartbeat(String serverAddress, int servicePort) {
		node = node.toBuilder().setHeartbeat(0).build();
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
