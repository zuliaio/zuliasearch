package io.zulia.util;

import io.zulia.message.ZuliaBase.Node;

/**
 * Identity key for a cluster node: server address and service port. Mutable Node fields such as
 * heartbeat and version never participate, so a key built from a stale Node snapshot still equals
 * a key built from the current registry entry for the same node.
 */
public record NodeKey(String serverAddress, int servicePort) {

	public static NodeKey of(Node node) {
		return new NodeKey(node.getServerAddress(), node.getServicePort());
	}

	/**
	 * Builds an identity-only Node carrying just the server address and service port. Useful where
	 * a Node message must persist identity without mutable fields like heartbeat or version.
	 */
	public Node toIdentityNode() {
		return Node.newBuilder().setServerAddress(serverAddress).setServicePort(servicePort).build();
	}

	@Override
	public String toString() {
		return serverAddress + ":" + servicePort;
	}
}
