package io.zulia.server.config;

import io.zulia.message.ZuliaBase.Node;

import java.util.List;

public interface NodeConfig {

	/**
	 *
	 * @return All nodes registered with a cluster
	 */
	List<Node> getNodes();

	/**
	 *
	 * @param serverAddress - sever address of node
	 * @param hazelcastPort - hazelcast port of node
	 * @return - full node data structure or null if doesn't exist
	 */
	Node getNode(String serverAddress, int hazelcastPort);

	/**
	 * Register a new node with a cluster
	 * Keyed on server name and hazelcast port
	 *
	 * @param node - new node to register with the cluster or node to update
	 */
	void addNode(Node node);

	/**
	 * Remove a node from the cluster
	 * @param serverAddress - sever address of node
	 * @param hazelcastPort - hazelcast port of node
	 */
	void removeNode(String serverAddress, int hazelcastPort);

}
