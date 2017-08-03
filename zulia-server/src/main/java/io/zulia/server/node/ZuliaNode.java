package io.zulia.server.node;

import io.zulia.server.config.NodeService;
import io.zulia.server.config.ZuliaConfig;
import io.zulia.server.hz.HazelcastManager;
import io.zulia.server.index.ZuliaIndexManager;
import io.zulia.server.rest.ZuliaRestServiceManager;

public class ZuliaNode {

	private final ZuliaIndexManager indexManager;
	private final ZuliaRestServiceManager restServiceManager;
	private final HazelcastManager hazelcastManager;

	public ZuliaNode(ZuliaConfig zuliaConfig, NodeService nodeService) {

		this.indexManager = new ZuliaIndexManager(zuliaConfig);
		this.restServiceManager = new ZuliaRestServiceManager(zuliaConfig, indexManager);

		this.hazelcastManager = new HazelcastManager(indexManager, nodeService);
	}


	public void start() {
		restServiceManager.start();
	}

	public void stop() {

		restServiceManager.stop();

		indexManager.shutdown();

		hazelcastManager.shutdown();
	}
}
