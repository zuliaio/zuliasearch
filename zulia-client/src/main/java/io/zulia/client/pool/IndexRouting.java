package io.zulia.client.pool;

import io.zulia.util.ShardUtil;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.zulia.message.ZuliaBase.Node;
import static io.zulia.message.ZuliaIndex.IndexMapping;
import static io.zulia.message.ZuliaIndex.ShardMapping;

public class IndexRouting {
	private Map<String, Map<Integer, Node>> indexMapping = new HashMap<>();
	private Map<String, Integer> segmentCountMapping = new HashMap<>();

	public IndexRouting(List<IndexMapping> indexMappingList) {
		for (IndexMapping im : indexMappingList) {
			Map<Integer, Node> segmentMapping = new HashMap<>();
			for (ShardMapping sg : im.getShardMappingList()) {
				// TODO: Does this need to know primary or replica?
				segmentMapping.put(sg.getShardNumber(), sg.getPrimayNode());
			}
			segmentCountMapping.put(im.getIndexName(), im.getNumberOfShards());
			indexMapping.put(im.getIndexName(), segmentMapping);
		}
	}

	public Node getNode(String indexName, String uniqueId) {
		Integer numberOfSegments = segmentCountMapping.get(indexName);
		if (numberOfSegments == null) {
			return null;
		}

		Map<Integer, Node> segmentMapping = indexMapping.get(indexName);

		int segmentNumber = ShardUtil.findShardForUniqueId(uniqueId, numberOfSegments);
		return segmentMapping.get(segmentNumber);
	}
}
