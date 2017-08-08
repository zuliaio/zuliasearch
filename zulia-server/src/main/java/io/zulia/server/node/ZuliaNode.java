package io.zulia.server.node;

import io.zulia.server.config.NodeService;
import io.zulia.server.config.ZuliaConfig;
import io.zulia.server.connection.ZuliaServiceServer;
import io.zulia.server.index.ZuliaIndexManager;
import io.zulia.server.rest.ZuliaRESTServiceManager;

import java.io.IOException;
import java.util.Collection;
import java.util.Timer;

import static io.zulia.message.ZuliaBase.Node;

public class ZuliaNode {

	private final ZuliaIndexManager indexManager;
	private final ZuliaRESTServiceManager restServiceManager;

	private final ZuliaServiceServer zuliaServiceServer;

	private final Timer membershipTimer;

	public ZuliaNode(ZuliaConfig zuliaConfig, NodeService nodeService) throws Exception {

		this.indexManager = new ZuliaIndexManager(zuliaConfig);
		this.restServiceManager = new ZuliaRESTServiceManager(zuliaConfig, indexManager);

		this.zuliaServiceServer = new ZuliaServiceServer(zuliaConfig, indexManager);

		membershipTimer = new Timer();

		membershipTimer.scheduleAtFixedRate(new MembershipTask(zuliaConfig, nodeService) {

			@Override
			protected void handleNodeRemove(Collection<Node> currentOtherNodesActive, Node removedNode) {
				indexManager.handleNodeRemoved(currentOtherNodesActive, removedNode);
			}

			@Override
			protected void handleNodeAdded(Collection<Node> currentOtherNodesActive, Node newNode) {
				indexManager.handleNodeAdded(currentOtherNodesActive, newNode);
			}
		}, 0, 1000);

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				membershipTimer.cancel();
				nodeService.removeHeartbeat(zuliaConfig.getServerAddress(), zuliaConfig.getServicePort());
				restServiceManager.shutdown();
				zuliaServiceServer.shutdown();
				indexManager.shutdown();
			}
		});

	}

	public void start() throws IOException {
		restServiceManager.start();
		zuliaServiceServer.start();
	}

}
