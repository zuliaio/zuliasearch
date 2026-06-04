package io.zulia.server.config.single;

import io.zulia.message.ZuliaBase.Node;
import io.zulia.server.config.NodeService;
import io.zulia.server.config.ZuliaConfig;
import io.zulia.server.node.ZuliaNode;
import io.zulia.util.ZuliaVersion;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

public class SingleNodeService implements NodeService {

	private final AtomicReference<Node> node = new AtomicReference<>();

	public SingleNodeService(ZuliaConfig zuliaConfig) {
		node.set(ZuliaNode.nodeFromConfig(zuliaConfig));
	}

	@Override
	public List<Node> getNodes() {

		return Collections.singletonList(node.get());
	}

	@Override
	public Node getNode(String serverAddress, int servicePort) {
		Node currentNode = node.get();
		if (currentNode.getServerAddress().equals(serverAddress) && currentNode.getServicePort() == servicePort) {
			return currentNode;
		}
		return null;
	}

	@Override
	public void updateVersion(String serverAddress, int servicePort, String version) {
		node.updateAndGet(current -> current.toBuilder().setVersion(ZuliaVersion.getVersion()).build());
	}

	@Override
	public void updateHeartbeat(String serverAddress, int servicePort) {
		node.updateAndGet(current -> current.toBuilder().setHeartbeat(System.currentTimeMillis()).build());
	}

	@Override
	public void removeHeartbeat(String serverAddress, int servicePort) {
		node.updateAndGet(current -> current.toBuilder().setHeartbeat(0).build());
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
