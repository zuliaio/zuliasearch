package io.zulia.server.node;

import io.zulia.server.config.NodeService;
import io.zulia.server.config.ZuliaConfig;
import io.zulia.server.connection.ZuliaServiceServer;
import io.zulia.server.hazelcast.HazelcastManager;
import io.zulia.server.index.ZuliaIndexManager;
import io.zulia.server.rest.ZuliaRESTServiceManager;

public class ZuliaNode {

	private final ZuliaIndexManager indexManager;
	private final ZuliaRESTServiceManager restServiceManager;
	private final HazelcastManager hazelcastManager;
	private final ZuliaServiceServer zuliaServiceServer;

	public ZuliaNode(ZuliaConfig zuliaConfig, NodeService nodeService) throws Exception {

		this.indexManager = new ZuliaIndexManager(zuliaConfig);
		this.restServiceManager = new ZuliaRESTServiceManager(zuliaConfig, indexManager);

		this.zuliaServiceServer = new ZuliaServiceServer(zuliaConfig, indexManager);

		this.hazelcastManager = new HazelcastManager(indexManager, nodeService, zuliaConfig);
	}

	public void start() {
		restServiceManager.start();
	}

	public void shutdown() {

		restServiceManager.shutdown();
		zuliaServiceServer.shutdown();
		indexManager.shutdown();
		hazelcastManager.shutdown();
	}
}
