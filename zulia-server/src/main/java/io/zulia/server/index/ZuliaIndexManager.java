package io.zulia.server.index;

import io.zulia.message.ZuliaBase.Node;
import io.zulia.server.config.IndexService;
import io.zulia.server.config.ZuliaConfig;
import io.zulia.server.config.cluster.MongoIndexService;
import io.zulia.server.config.single.FSIndexService;
import io.zulia.server.util.MongoProvider;

import java.util.Set;

public class ZuliaIndexManager {

	private final IndexService indexService;

	public ZuliaIndexManager(ZuliaConfig zuliaConfig) {

		if (zuliaConfig.isCluster()) {
			indexService = new MongoIndexService(MongoProvider.getMongoClient());
		}
		else {
			indexService = new FSIndexService();
		}
	}

	public void handleNodeAdded(Set<Node> currentNodes, Node nodeAdded, boolean master) {

	}

	public void handleNodeRemoved(Set<Node> currentNodes, Node nodeRemoved, boolean master) {

	}

	public void shutdown() {

	}
}
