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
				segmentMapping.put(sg.getShardNumber(), sg.getPrimaryNode());
			}
			shardCountMapping.put(im.getIndexName(), im.getNumberOfShards());
			indexMapping.put(im.getIndexName(), segmentMapping);
		}
	}

	public Node getNode(String indexName, String uniqueId) {
		Integer numberOfShards = shardCountMapping.get(indexName);
		if (numberOfShards == null) {
			return null;
		}

		Map<Integer, Node> shardMapping = indexMapping.get(indexName);

		if (shardMapping.isEmpty()) {
			return null;
		}

		int shardNumber = ShardUtil.findShardForUniqueId(uniqueId, numberOfShards);
		return shardMapping.get(shardNumber);
	}

	public Node getRandomNode(String indexName) {
		Integer numberOfShards = shardCountMapping.get(indexName);
		if (numberOfShards == null) {
			return null;
		}

		Map<Integer, Node> shardMapping = indexMapping.get(indexName);

		if (shardMapping.isEmpty()) {
			return null;
		}

		int shardNumber = random.nextInt(numberOfShards);
		return shardMapping.get(shardNumber);
	}

	public Node getRandomNode(Collection<String> indexNames) {

		Set<Node> allNodes = new HashSet<>();

		for (String indexName : indexNames) {
			Integer numberOfShards = shardCountMapping.get(indexName);
			if (numberOfShards == null) {
				return null;
			}

			Map<Integer, Node> shardMapping = indexMapping.get(indexName);
			allNodes.addAll(shardMapping.values());

		}

		if (allNodes.isEmpty()) {
			return null;
		}

		List<Node> allNodesList = new ArrayList<>(allNodes);

		int nodeIndex = random.nextInt(allNodesList.size());
		return allNodesList.get(nodeIndex);
	}

}
