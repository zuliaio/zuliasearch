package io.zulia.server.index;

import io.zulia.message.ZuliaBase.Node;
import io.zulia.server.config.IndexService;
import io.zulia.server.config.ZuliaConfig;
import io.zulia.server.config.cluster.MongoIndexService;
import io.zulia.server.config.single.FSIndexService;
import io.zulia.server.util.MongoProvider;

import java.util.Collection;
import java.util.logging.Logger;

public class ZuliaIndexManager {

	private static final Logger LOG = Logger.getLogger(ZuliaIndexManager.class.getName());

	private final IndexService indexService;

	public ZuliaIndexManager(ZuliaConfig zuliaConfig) {

		if (zuliaConfig.isCluster()) {
			indexService = new MongoIndexService(MongoProvider.getMongoClient());
		}
		else {
			indexService = new FSIndexService();
		}
	}

	public void handleNodeAdded(Collection<Node> currentNodes, Node nodeAdded, boolean master) {

	}

	public void handleNodeRemoved(Collection<Node> currentNodes, Node nodeRemoved, boolean master) {

	}

	public void shutdown() {

	}

	public void loadIndexes() {

	}

	public void openConnections(Collection<Node> nodes) {

	}
}
