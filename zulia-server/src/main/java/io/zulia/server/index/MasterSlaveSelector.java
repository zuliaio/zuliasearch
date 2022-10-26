package io.zulia.server.index;

import io.zulia.message.ZuliaBase.MasterSlaveSettings;
import io.zulia.message.ZuliaBase.Node;
import io.zulia.message.ZuliaIndex.IndexShardMapping;
import io.zulia.message.ZuliaIndex.ShardMapping;
import io.zulia.message.ZuliaServiceOuterClass.IndexRouting;
import io.zulia.server.exceptions.ShardDoesNotExistException;
import io.zulia.server.exceptions.ShardOfflineException;
import io.zulia.server.node.ZuliaNode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MasterSlaveSelector {

	private final MasterSlaveSettings masterSlaveSettings;
	private final List<Node> nodes;
	private IndexShardMapping indexMapping;

	/**
	 * @param masterSlaveSettings - the master slave preference
	 * @param nodes               - list of nodes to select from, order of the list determines secondary that is selected
	 * @param indexMapping        -
	 */
	public MasterSlaveSelector(MasterSlaveSettings masterSlaveSettings, List<Node> nodes, IndexShardMapping indexMapping) {
		this.masterSlaveSettings = masterSlaveSettings;
		this.nodes = nodes;
		this.indexMapping = indexMapping;
	}

	public Node getNodeForUniqueId(String uniqueId) throws ShardDoesNotExistException, ShardOfflineException {
		int shardForUniqueId = getShardForUniqueId(uniqueId, indexMapping.getNumberOfShards());

		for (ShardMapping shardMapping : indexMapping.getShardMappingList()) {
			if (shardMapping.getShardNumber() == shardForUniqueId) {
				return getNodeFromShardMapping(shardMapping);
			}
		}

		throw new ShardDoesNotExistException(indexMapping.getIndexName(), shardForUniqueId);

	}

	public Map<Node, IndexRouting.Builder> getNodesForIndex() throws ShardOfflineException {

		Map<Node, IndexRouting.Builder> map = new HashMap<>();
		for (io.zulia.message.ZuliaIndex.ShardMapping shardMapping : indexMapping.getShardMappingList()) {
			Node selectedNode = getNodeFromShardMapping(shardMapping);
			map.computeIfAbsent(selectedNode, (k) -> IndexRouting.newBuilder()).addShard(shardMapping.getShardNumber());
		}

		return map;
	}

	protected Node getNodeFromShardMapping(ShardMapping shardMapping) throws ShardOfflineException {
		Node selectedNode;
		if (MasterSlaveSettings.MASTER_ONLY.equals(masterSlaveSettings)) {
			selectedNode = getSelectMasterNode(shardMapping);
			if (selectedNode == null) {
				throw new ShardOfflineException(indexMapping.getIndexName(), shardMapping.getShardNumber(), masterSlaveSettings);
			}

		}
		else if (MasterSlaveSettings.SLAVE_ONLY.equals(masterSlaveSettings)) {
			selectedNode = getSelectSlaveNode(shardMapping);
			if (selectedNode == null) {
				throw new ShardOfflineException(indexMapping.getIndexName(), shardMapping.getShardNumber(), masterSlaveSettings);
			}
		}
		else if (MasterSlaveSettings.MASTER_IF_AVAILABLE.equals(masterSlaveSettings)) {
			selectedNode = getSelectMasterNode(shardMapping);
			if (selectedNode == null) {
				selectedNode = getSelectSlaveNode(shardMapping);
				if (selectedNode == null) {
					throw new ShardOfflineException(indexMapping.getIndexName(), shardMapping.getShardNumber(), masterSlaveSettings);
				}
			}
		}
		else {
			throw new IllegalArgumentException("Unknown master slave setting");
		}
		return selectedNode;
	}

	protected Node getSelectMasterNode(ShardMapping shardMapping) {
		Node selectedNode = null;

		for (Node onlineNode : nodes) {
			if (ZuliaNode.isEqual(onlineNode, shardMapping.getPrimaryNode())) {
				selectedNode = onlineNode;
				break;
			}
		}

		return selectedNode;
	}

	protected Node getSelectSlaveNode(ShardMapping shardMapping) {

		for (Node secondaryNode : shardMapping.getReplicaNodeList()) {
			for (Node onlineNode : nodes) {
				if (ZuliaNode.isEqual(onlineNode, secondaryNode)) {
					return onlineNode;
				}
			}
		}

		return null;
	}

	public static int getShardForUniqueId(String uniqueId, int numOfShards) {
		return Math.abs(uniqueId.hashCode()) % numOfShards;
	}
}
