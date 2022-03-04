package io.zulia.server.config;

import io.zulia.message.ZuliaBase.Node;

import java.util.Collection;

public interface NodeService {

	/**
	 *
	 * @return All nodes registered with a cluster
	 */
	Collection<Node> getNodes();

	/**
	 *
	 * @param serverAddress - sever address of node
	 * @param servicePort - service port of node
	 * @return - full node data structure or null if doesn't exist
	 */
	Node getNode(String serverAddress, int servicePort);

	/**
	 * Update heartbeat for a cluster node
	 * @param serverAddress - server address of node
	 * @param servicePort - service port of node
	 */
	void updateHeartbeat(String serverAddress, int servicePort);

	/**
	 * Update version for a cluster node
	 * @param version - version of node
	 */
	void updateVersion(String serverAddress, int servicePort, String version);

	/**
	 * Removed heartbeat for a cluster node
	 * @param serverAddress - server address of node
	 * @param servicePort - service port of node
	 */
	void removeHeartbeat(String serverAddress, int servicePort);

	/**
	 * Register a new node with a cluster
	 * Keyed on server name and service port
	 *
	 * @param node - new node to register with the cluster or node to update
	 */
	void addNode(Node node);

	/**
	 * Remove a node from the cluster
	 * @param serverAddress - server address of node
	 * @param servicePort - service port of node
	 */
	void removeNode(String serverAddress, int servicePort);

}
