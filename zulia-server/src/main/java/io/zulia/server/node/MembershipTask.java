package io.zulia.server.node;

import io.zulia.message.ZuliaBase.Node;
import io.zulia.server.config.NodeService;
import io.zulia.server.config.ZuliaConfig;
import io.zulia.server.index.ZuliaIndexManager;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimerTask;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public abstract class MembershipTask extends TimerTask {

	//a node is no longer considered alive if it has not updated its heartbeat for this number of seconds
	public static final int MAX_HEARTBEAT_LAG_SECONDS = 30;

	private final NodeService nodeService;
	private final ZuliaConfig zuliaConfig;

	private Map<String, Node> otherNodeMap;

	private static final Logger LOG = Logger.getLogger(ZuliaIndexManager.class.getName());

	public MembershipTask(ZuliaConfig zuliaConfig, NodeService nodeService) {
		this.nodeService = nodeService;
		this.zuliaConfig = zuliaConfig;
		this.otherNodeMap = new HashMap<>();
	}

	@Override
	public void run() {

		try {
			//update this node's heartbeat
			nodeService.updateHeartbeat(zuliaConfig.getServerAddress(), zuliaConfig.getServicePort());

			//get list of other cluster members and sort by heartbeat time descending
			//members never start with have heartbeat time of 0
			ArrayList<Node> nodesList = new ArrayList<>(nodeService.getNodes());
			nodesList.sort(Comparator.comparingLong(Node::getHeartbeat).reversed());

			Map<String, Node> newOtherNodeMap = new HashMap<>();

			long latest = nodesList.get(0).getHeartbeat();
			for (Node node : nodesList) {
				if (latest - node.getHeartbeat() < MAX_HEARTBEAT_LAG_SECONDS * 1000) {
					if (node.getServerAddress().equals(zuliaConfig.getServerAddress()) && (node.getServicePort() == zuliaConfig.getServicePort())) {
						//skip this server
					}
					else {
						newOtherNodeMap.put(node.getServerAddress() + ":" + node.getServicePort(), node);
					}
				}
			}

			List<Node> removedNodesList = Collections.emptyList();
			List<Node> newNodesList = Collections.emptyList();

			{
				Set<String> removedNodes = new HashSet<>(otherNodeMap.keySet());
				removedNodes.removeAll(newOtherNodeMap.keySet());

				if (!removedNodes.isEmpty()) {
					removedNodesList = removedNodes.stream().map(otherNodeMap::get).collect(Collectors.toList());
				}

				Set<String> newNodes = new HashSet<>(newOtherNodeMap.keySet());
				newNodes.removeAll(otherNodeMap.keySet());

				if (!newNodes.isEmpty()) {
					newNodesList = newNodes.stream().map(newOtherNodeMap::get).collect(Collectors.toList());
				}

			}

			otherNodeMap = newOtherNodeMap;

			if (!newNodesList.isEmpty() || !removedNodesList.isEmpty()) {
				ArrayList<Node> otherNodes = new ArrayList<>(otherNodeMap.values());

				for (Node removedNode : removedNodesList) {
					handleNodeRemove(otherNodes, removedNode);
				}

				for (Node newNode : newNodesList) {
					handleNodeAdded(otherNodes, newNode);
				}
			}

		}
		catch (Throwable t) {
			LOG.log(Level.SEVERE, "Update membership failed: ", t);
		}
	}

	protected abstract void handleNodeRemove(Collection<Node> currentOtherNodesActive, Node removedNode);

	protected abstract void handleNodeAdded(Collection<Node> currentOtherNodesActive, Node newNode);

}
