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
	 * Register a new node with a cluster
	 * Keyed on server name and hazelcast port
	 *
	 * @param node - new node to register with the cluster or node to update
	 */
	void addNode(Node node);

	/**
	 * Removed a node from the cluster
	 * Keyed on server name and hazelcast port
	 * @param node - node to remove from the cluster
	 */
	void removeNode(Node node);

}
