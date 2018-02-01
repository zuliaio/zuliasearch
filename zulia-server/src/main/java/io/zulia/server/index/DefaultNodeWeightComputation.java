package io.zulia.server.index;

import com.google.common.util.concurrent.AtomicDouble;
import io.zulia.message.ZuliaBase.Node;
import io.zulia.message.ZuliaIndex;
import io.zulia.message.ZuliaIndex.IndexMapping;
import io.zulia.message.ZuliaIndex.IndexSettings;
import io.zulia.server.config.IndexService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class DefaultNodeWeightComputation implements NodeWeightComputation {

	public static final double REPLICA_WEIGHT_DELTA = 0.01;

	private final HashMap<Node, AtomicDouble> nodeWeightMap;

	public DefaultNodeWeightComputation(IndexService indexService, Node thisNode, Collection<Node> currentOtherNodesActive) throws Exception {

		Set<Node> activeNodes = new HashSet<>();
		activeNodes.add(thisNode);
		activeNodes.addAll(currentOtherNodesActive);

		HashMap<String, Integer> weightMap = new HashMap<>();

		for (IndexSettings settings : indexService.getIndexes()) {

			weightMap.put(settings.getIndexName(), settings.getIndexWeight());

		}

		nodeWeightMap = new HashMap<>();

		for (IndexMapping indexMapping : indexService.getIndexMappings()) {
			Integer weightForIndex = weightMap.get(indexMapping.getIndexName());
			if (weightForIndex == null) {
				weightForIndex = 1;
				//TODO fix this
				System.out.println(indexMapping.getIndexName() + " has no weight");
			}
			double indexShardWeight = weightForIndex / indexMapping.getNumberOfShards();
			for (ZuliaIndex.ShardMapping shardMapping : indexMapping.getShardMappingList()) {
				nodeWeightMap.computeIfAbsent(getNodeKey(shardMapping.getPrimaryNode()), k -> new AtomicDouble()).addAndGet(indexShardWeight);

				for (Node node : shardMapping.getReplicaNodeList()) {
					nodeWeightMap.computeIfAbsent(getNodeKey(node), k -> new AtomicDouble()).addAndGet(indexShardWeight - REPLICA_WEIGHT_DELTA);
				}
			}
		}

		for (Node node : activeNodes) {
			nodeWeightMap.computeIfAbsent(getNodeKey(node), k -> new AtomicDouble());
		}

		nodeWeightMap.keySet().retainAll(activeNodes.stream().map(this::getNodeKey).collect(Collectors.toSet()));

	}

	@Override
	public List<Node> getNodesSortedByWeight() {
		List<Map.Entry<Node, AtomicDouble>> entries = new ArrayList<>(nodeWeightMap.entrySet());
		Collections.shuffle(entries);
		entries.sort(Map.Entry.comparingByValue(Comparator.comparingDouble(AtomicDouble::get)));
		return entries.stream().map(Map.Entry::getKey).collect(Collectors.toList());
	}

	private Node getNodeKey(Node primaryNode) {
		return Node.newBuilder().setServerAddress(primaryNode.getServerAddress()).setServicePort(primaryNode.getServicePort()).build();
	}

	@Override
	public void addShard(Node node, IndexSettings indexSettings, boolean primary) {

		int indexWeight = indexSettings.getIndexWeight() != 0 ? indexSettings.getIndexWeight() : 1;
		double indexShardWeight = (double) indexWeight / indexSettings.getNumberOfShards();
		if (!primary) {
			indexShardWeight = Math.max(0, indexShardWeight - REPLICA_WEIGHT_DELTA);
		}
		nodeWeightMap.computeIfAbsent(node, k -> new AtomicDouble()).addAndGet(indexShardWeight);

	}
}

