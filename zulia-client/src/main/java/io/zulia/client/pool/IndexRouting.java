package io.zulia.client.pool;

import io.zulia.message.ZuliaIndex.IndexAlias;
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
import static io.zulia.message.ZuliaIndex.IndexShardMapping;
import static io.zulia.message.ZuliaIndex.ShardMapping;

public class IndexRouting {

	private final Random random = new Random();

	private final Map<String, String> aliasToIndex;
	private final Map<String, Map<Integer, Node>> indexMapping;
	private final Map<String, Integer> shardCountMapping;

	public IndexRouting(List<IndexShardMapping> indexShardMappings, List<IndexAlias> indexAliases) {

		aliasToIndex = new HashMap<>();
		indexMapping = new HashMap<>();
		shardCountMapping = new HashMap<>();

		for (IndexAlias indexAlias : indexAliases) {
			aliasToIndex.put(indexAlias.getAliasName(), indexAlias.getIndexName());
		}

		for (IndexShardMapping ism : indexShardMappings) {
			Map<Integer, Node> segmentMapping = new HashMap<>();
			for (ShardMapping sg : ism.getShardMappingList()) {
				// TODO: Does this need to know primary or replica?
				segmentMapping.put(sg.getShardNumber(), sg.getPrimaryNode());
			}
			shardCountMapping.put(ism.getIndexName(), ism.getNumberOfShards());
			indexMapping.put(ism.getIndexName(), segmentMapping);
		}
	}

	public Node getNode(String indexName, String uniqueId) {

		indexName = handleAlias(indexName);

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
		indexName = handleAlias(indexName);

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
			indexName = handleAlias(indexName);

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

	private String handleAlias(String indexName) {
		if (aliasToIndex.containsKey(indexName)) {
			indexName = aliasToIndex.get(indexName);
		}
		return indexName;
	}

}
