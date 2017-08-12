package io.zulia.server.index;

import io.zulia.message.ZuliaBase.MasterSlaveSettings;
import io.zulia.message.ZuliaBase.Node;
import io.zulia.message.ZuliaIndex.ShardMapping;
import io.zulia.server.exceptions.ShardOfflineException;
import io.zulia.server.node.ZuliaNode;

import java.io.IOException;
import java.util.Collection;

public abstract class RequestNodeBase<I, O> {

	private final Node thisNode;
	private final Collection<Node> otherNodesActive;
	private final MasterSlaveSettings masterSlaveSettings;

	public RequestNodeBase(Node thisNode, Collection<Node> otherNodesActive, MasterSlaveSettings masterSlaveSettings) throws IOException {

		this.thisNode = thisNode;
		this.otherNodesActive = otherNodesActive;
		this.masterSlaveSettings = masterSlaveSettings;

	}

	protected Node getNodeFromShardMapping(String indexName, ShardMapping shardMapping) throws ShardOfflineException {
		Node selectedNode;
		if (MasterSlaveSettings.MASTER_ONLY.equals(masterSlaveSettings)) {
			selectedNode = getSelectMasterNode(shardMapping);
			if (selectedNode == null) {
				throw new ShardOfflineException(indexName, shardMapping.getShardNumber(), masterSlaveSettings);
			}

		}
		else if (MasterSlaveSettings.SLAVE_ONLY.equals(masterSlaveSettings)) {
			selectedNode = getSelectSlaveNode(shardMapping);
			if (selectedNode == null) {
				throw new ShardOfflineException(indexName, shardMapping.getShardNumber(), masterSlaveSettings);
			}
		}
		else if (MasterSlaveSettings.MASTER_IF_AVAILABLE.equals(masterSlaveSettings)) {
			selectedNode = getSelectMasterNode(shardMapping);
			if (selectedNode == null) {
				selectedNode = getSelectSlaveNode(shardMapping);
				if (selectedNode == null) {
					throw new ShardOfflineException(indexName, shardMapping.getShardNumber(), masterSlaveSettings);
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

	protected Node getSelectSlaveNode(ShardMapping shardMapping) {

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

	public boolean nodeIsLocal(Node node) {
		return ZuliaNode.isEqual(node, thisNode);
	}

	protected abstract O processExternal(Node node, I request) throws Exception;

	protected abstract O processInternal(I request) throws Exception;

}
