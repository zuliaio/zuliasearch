package io.zulia.server.health;

import io.micronaut.health.HealthStatus;
import io.micronaut.management.health.indicator.AbstractHealthIndicator;
import io.micronaut.management.health.indicator.HealthResult;
import io.zulia.message.ZuliaBase.Node;
import io.zulia.message.ZuliaIndex.IndexShardMapping;
import io.zulia.message.ZuliaIndex.ShardMapping;
import io.zulia.message.ZuliaServiceOuterClass.GetNodesRequest;
import io.zulia.message.ZuliaServiceOuterClass.GetNodesResponse;
import io.zulia.server.node.ZuliaNode;
import jakarta.inject.Singleton;
import org.jspecify.annotations.NullMarked;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Singleton
@NullMarked
public class IndexHealthIndicator extends AbstractHealthIndicator<Map<String, Object>> {

	private static final Logger LOG = LoggerFactory.getLogger(IndexHealthIndicator.class);

	private final ZuliaNode zuliaNode;

	public IndexHealthIndicator(ZuliaNode zuliaNode) {
		this.zuliaNode = zuliaNode;
	}

	@Override
	protected Map<String, Object> getHealthInformation() {
		Map<String, Object> details = new LinkedHashMap<>();
		List<String> offlineShards = new ArrayList<>();

		try {
			GetNodesResponse nodesResponse = zuliaNode.getIndexManager().getNodes(GetNodesRequest.newBuilder().setActiveOnly(true).build());
			List<Node> onlineNodes = nodesResponse.getNodeList();

			for (IndexShardMapping indexShardMapping : nodesResponse.getIndexShardMappingList()) {
				String indexName = indexShardMapping.getIndexName();

				for (ShardMapping shardMapping : indexShardMapping.getShardMappingList()) {
					int shardNumber = shardMapping.getShardNumber();

					boolean primaryOnline = isNodeOnline(shardMapping.getPrimaryNode(), onlineNodes);
					if (!primaryOnline) {
						offlineShards.add(indexName + ":s" + shardNumber + ":primary");
					}

					for (Node replicaNode : shardMapping.getReplicaNodeList()) {
						boolean replicaOnline = isNodeOnline(replicaNode, onlineNodes);
						if (!replicaOnline) {
							offlineShards.add(indexName + ":s" + shardNumber + ":replica");
						}
					}
				}
			}
		}
		catch (Exception e) {
			LOG.error("Failed to check index shard health", e);
			healthStatus = HealthStatus.DOWN.describe("Failed to check index shard health: " + e.getMessage());
			details.put("error", e.getMessage());
			return details;
		}

		if (offlineShards.isEmpty()) {
			healthStatus = HealthStatus.UP.describe("All index shards online");
		}
		else {
			healthStatus = HealthStatus.DOWN.describe(offlineShards.size() + " shard(s) offline");
			LOG.warn("Offline shards detected: {}", offlineShards);
		}

		details.put("offlineShards", offlineShards);
		details.put("offlineCount", offlineShards.size());
		return details;
	}

	private static boolean isNodeOnline(Node node, List<Node> onlineNodes) {
		for (Node onlineNode : onlineNodes) {
			if (ZuliaNode.isEqual(onlineNode, node)) {
				return true;
			}
		}
		return false;
	}

	@Override
	protected String getName() {
		return "indexShards";
	}

	@Override
	public HealthResult getHealthResult() {
		return super.getHealthResult();
	}
}
