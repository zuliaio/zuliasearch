package io.zulia.server.index;

import io.zulia.message.ZuliaBase;
import io.zulia.message.ZuliaBase.Node;
import io.zulia.message.ZuliaIndex.IndexMapping;
import io.zulia.server.exceptions.ShardOfflineException;
import io.zulia.server.node.ZuliaNode;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public abstract class RequestNodeFederator<I, O> {

	private final Set<Node> nodes;
	private final Node thisNode;
	private final Collection<ZuliaIndex> indexes;
	private final Collection<Node> otherNodesActive;
	private ExecutorService pool;

	public RequestNodeFederator(Node thisNode, Collection<Node> otherNodesActive, ExecutorService pool, Collection<ZuliaIndex> indexes,
			ZuliaBase.MasterSlaveSettings masterSlaveSettings) throws IOException {

		this.pool = pool;
		this.thisNode = thisNode;
		this.otherNodesActive = otherNodesActive;
		this.indexes = indexes;

		this.nodes = new HashSet<>();

		for (ZuliaIndex index : indexes) {
			IndexMapping indexMapping = index.getIndexMapping();

			for (int s = 0; s < index.getNumberOfShards(); s++) {
				for (io.zulia.message.ZuliaIndex.ShardMapping shardMapping : indexMapping.getShardMappingList()) {
					if (ZuliaBase.MasterSlaveSettings.MASTER_ONLY.equals(masterSlaveSettings)) {
						Node selectedNode = getSelectMasterNode(shardMapping);
						if (selectedNode == null) {
							throw new ShardOfflineException(index.getIndexName(), s, masterSlaveSettings);
						}
						nodes.add(selectedNode);
					}
					else if (ZuliaBase.MasterSlaveSettings.SLAVE_ONLY.equals(masterSlaveSettings)) {
						Node selectedNode = getSelectSlaveNode(shardMapping);
						if (selectedNode == null) {
							throw new ShardOfflineException(index.getIndexName(), s, masterSlaveSettings);
						}
						nodes.add(selectedNode);
					}
					if (ZuliaBase.MasterSlaveSettings.MASTER_IF_AVAILABLE.equals(masterSlaveSettings)) {
						Node selectedNode = getSelectMasterNode(shardMapping);
						if (selectedNode == null) {
							selectedNode = getSelectSlaveNode(shardMapping);
							if (selectedNode == null) {
								throw new ShardOfflineException(index.getIndexName(), s, masterSlaveSettings);
							}
							nodes.add(selectedNode);
						}
					}

				}
			}
		}

	}

	private Node getSelectMasterNode(io.zulia.message.ZuliaIndex.ShardMapping shardMapping) {
		Node selectedNode = null;

		if (ZuliaNode.isEqual(shardMapping.getPrimayNode(), thisNode)) {
			selectedNode = thisNode;
		}
		else {
			for (Node onlineNode : otherNodesActive) {
				if (ZuliaNode.isEqual(onlineNode, shardMapping.getPrimayNode())) {
					selectedNode = onlineNode;
					break;
				}
			}
		}
		return selectedNode;
	}

	private Node getSelectSlaveNode(io.zulia.message.ZuliaIndex.ShardMapping shardMapping) {

		for (Node secondaryNode : shardMapping.getReplicaNodeList()) {
			if (ZuliaNode.isEqual(secondaryNode, thisNode)) {
				return thisNode;
			}
		}

		for (Node secondaryNode : shardMapping.getReplicaNodeList()) {
			for (Node onlineNode : otherNodesActive) {
				if (ZuliaNode.isEqual(onlineNode, secondaryNode)) {
					return onlineNode;
				}
			}
		}

		return null;
	}

	public List<O> send(final I request) throws Exception {

		List<Future<O>> futureResponses = new ArrayList<>();

		for (final Node node : nodes) {

			Future<O> futureResponse = pool.submit(() -> {

				if (node.getServerAddress().equals(thisNode.getServerAddress()) && node.getServicePort() == thisNode.getServicePort()) {
					return processInternal(request);
				}
				return processExternal(node, request);

			});

			futureResponses.add(futureResponse);
		}

		ArrayList<O> results = new ArrayList<O>();
		for (Future<O> response : futureResponses) {
			try {
				O result = response.get();
				results.add(result);
			}
			catch (InterruptedException e) {
				throw new Exception("Interrupted while waiting for results");
			}
			catch (Exception e) {
				Throwable cause = e.getCause();
				if (cause instanceof Exception) {
					throw (Exception) cause;
				}

				throw e;
			}
		}

		return results;

	}

	public abstract O processExternal(Node node, I request) throws Exception;

	public abstract O processInternal(I request) throws Exception;
}
