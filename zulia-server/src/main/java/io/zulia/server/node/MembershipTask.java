package io.zulia.server.node;

import io.zulia.message.ZuliaBase.Node;
import io.zulia.server.config.NodeService;
import io.zulia.server.config.ZuliaConfig;
import io.zulia.util.NodeKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public abstract class MembershipTask extends TimerTask {

	private static final Logger LOG = LoggerFactory.getLogger(MembershipTask.class);
	//a node is no longer considered alive if it has not updated its heartbeat for this number of seconds
	public static final int MAX_HEARTBEAT_LAG_SECONDS = 30;
	private final NodeService nodeService;
	private final ZuliaConfig zuliaConfig;
	private final NodeKey thisNodeKey;
	private Map<NodeKey, Node> otherNodeMap;
	// heartbeat publishing is deferred until the node can actually serve
	// advertising during index loading makes peers route to a port that is not yet listening, failing every federated request
	// that touches this node's shards instead of letting replicas elsewhere answer
	private volatile boolean advertise;

	public MembershipTask(ZuliaConfig zuliaConfig, NodeService nodeService) {
		this.nodeService = nodeService;
		this.zuliaConfig = zuliaConfig;
		this.thisNodeKey = new NodeKey(zuliaConfig.getServerAddress(), zuliaConfig.getServicePort());
		this.otherNodeMap = new HashMap<>();
	}

	/**
	 * Starts publishing this node's heartbeat. Until this is called the task runs read-only,
	 * keeping the local view of other cluster members current without advertising this node.
	 */
	public void startAdvertising() {
		advertise = true;
	}

	@Override
	public synchronized void run() {
		// synchronized: the startup thread calls run() directly (initial read-only pass and the
		// post-advertise publish) while the timer thread runs the scheduled ticks, and the body
		// mutates otherNodeMap and fires add/remove callbacks assuming single-threaded execution

		try {
			if (advertise) {
				//update this node's heartbeat
				nodeService.updateHeartbeat(zuliaConfig.getServerAddress(), zuliaConfig.getServicePort());
			}

			//get list of other cluster members and sort by heartbeat time descending
			//members never start with have heartbeat time of 0
			ArrayList<Node> nodesList = new ArrayList<>(nodeService.getNodes());
			nodesList.sort(Comparator.comparingLong(Node::getHeartbeat).reversed());

			Map<NodeKey, Node> newOtherNodeMap = new HashMap<>();
			Map<NodeKey, Node> currentNodesByKey = new HashMap<>();

			// anchor the lag filter in the heartbeat clock domain. While advertising, this node's own
			// publish at the top of this tick keeps the list maximum fresh. In the read-only startup
			// window a whole-cluster crash leaves only stale heartbeats, where the newest crashed peer
			// would read as active (lag 0 against its own anchor), so probe the database for a fresh
			// timestamp instead. In cluster mode, the local clock never participates in cluster mode and
			// Heartbeats are stamped with the database clock, and an anchor from this node's clock would
			// expire every peer whenever it ran more than the lag window ahead of the database
			long listMax = nodesList.isEmpty() ? 0 : nodesList.getFirst().getHeartbeat();
			long anchor = advertise ? listMax : Math.max(nodeService.getClusterTime(zuliaConfig.getServerAddress(), zuliaConfig.getServicePort()), listMax);
			for (Node node : nodesList) {
				NodeKey nodeKey = NodeKey.of(node);
				currentNodesByKey.put(nodeKey, node);
				// registered but never started, previously filtered only by lag against this node's own
				// fresh heartbeat, which deferred advertising no longer guarantees is in the list
				if (node.getHeartbeat() == 0) {
					continue;
				}
				if (anchor - node.getHeartbeat() < MAX_HEARTBEAT_LAG_SECONDS * 1000) {
					if (nodeKey.equals(thisNodeKey)) {
						//skip this server
					}
					else {
						newOtherNodeMap.put(nodeKey, node);
					}
				}
			}

			List<Node> removedNodesList = Collections.emptyList();
			List<Node> newNodesList = Collections.emptyList();

			{
				Set<NodeKey> removedNodes = new HashSet<>(otherNodeMap.keySet());
				removedNodes.removeAll(newOtherNodeMap.keySet());

				if (!removedNodes.isEmpty()) {
					removedNodesList = removedNodes.stream().map(otherNodeMap::get).toList();
				}

				Set<NodeKey> newNodes = new HashSet<>(newOtherNodeMap.keySet());
				newNodes.removeAll(otherNodeMap.keySet());

				if (!newNodes.isEmpty()) {
					newNodesList = newNodes.stream().map(newOtherNodeMap::get).toList();
				}

			}

			otherNodeMap = newOtherNodeMap;

			if (!newNodesList.isEmpty() || !removedNodesList.isEmpty()) {
				ArrayList<Node> otherNodes = new ArrayList<>(otherNodeMap.values());

				for (Node removedNode : removedNodesList) {
					// judge against the node's current registry entry: the otherNodeMap snapshot always
					// holds the last heartbeat this node saw while the peer was alive, never 0
					Node current = currentNodesByKey.get(NodeKey.of(removedNode));
					if (current == null || current.getHeartbeat() == 0) {
						LOG.info("Removing node {}:{} that shut down cleanly or was deregistered", removedNode.getServerAddress(),
								removedNode.getServicePort());
					}
					else {
						LOG.info("Removing expired node {}:{} - node heartbeat: {}, anchor: {}, lag: {}ms", removedNode.getServerAddress(),
								removedNode.getServicePort(), current.getHeartbeat(), anchor, anchor - current.getHeartbeat());
					}
					handleNodeRemove(otherNodes, removedNode);
				}

				for (Node newNode : newNodesList) {
					handleNodeAdded(otherNodes, newNode);
				}
			}

		}
		catch (Throwable t) {
			LOG.error("Update membership failed: ", t);
		}
	}

	protected abstract void handleNodeRemove(Collection<Node> currentOtherNodesActive, Node removedNode);

	protected abstract void handleNodeAdded(Collection<Node> currentOtherNodesActive, Node newNode);

}
