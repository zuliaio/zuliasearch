package io.zulia.client.pool;

import io.zulia.util.ShardUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static io.zulia.message.ZuliaBase.Node;
import static io.zulia.message.ZuliaIndex.IndexMapping;
import static io.zulia.message.ZuliaIndex.ShardMapping;

public class IndexRouting {

	private Random random = new Random();

	private Map<String, Map<Integer, Node>> indexMapping = new HashMap<>();
	private Map<String, Integer> shardCountMapping = new HashMap<>();

	public IndexRouting(List<IndexMapping> indexMappingList) {
		for (IndexMapping im : indexMappingList) {
			Map<Integer, Node> segmentMapping = new HashMap<>();
			for (ShardMapping sg : im.getShardMappingList()) {
				// TODO: Does this need to know primary or replica?
				segmentMapping.put(sg.getShardNumber(), sg.getPrimayNode());
			}
			shardCountMapping.put(im.getIndexName(), im.getNumberOfShards());
			indexMapping.put(im.getIndexName(), segmentMapping);
		}
	}

	public Node getNode(String indexName, String uniqueId) {
		Integer numberOfSegments = shardCountMapping.get(indexName);
		if (numberOfSegments == null) {
			return null;
		}

		Map<Integer, Node> segmentMapping = indexMapping.get(indexName);

		if (segmentMapping.isEmpty()) {
			return null;
		}

		int segmentNumber = ShardUtil.findShardForUniqueId(uniqueId, numberOfSegments);
		return segmentMapping.get(segmentNumber);
	}

	public Node getRandomNode(String indexName) {
		Integer numberOfSegments = shardCountMapping.get(indexName);
		if (numberOfSegments == null) {
			return null;
		}

		Map<Integer, Node> segmentMapping = indexMapping.get(indexName);

		if (segmentMapping.isEmpty()) {
			return null;
		}

		int segmentNumber = random.nextInt(numberOfSegments);
		return segmentMapping.get(segmentNumber);
	}

	public Node getRandomNode(Collection<String> indexNames) {

		Set<Node> allNodes = new HashSet<>();

		for (String indexName : indexNames) {
			Integer numberOfSegments = shardCountMapping.get(indexName);
			if (numberOfSegments == null) {
				return null;
			}

			Map<Integer, Node> segmentMapping = indexMapping.get(indexName);
			allNodes.addAll(segmentMapping.values());

		}

		if (allNodes.isEmpty()) {
			return null;
		}

		List<Node> allNodesList = new ArrayList<>(allNodes);

		int nodeIndex = random.nextInt(allNodesList.size());
		return allNodesList.get(nodeIndex);
	}

}
