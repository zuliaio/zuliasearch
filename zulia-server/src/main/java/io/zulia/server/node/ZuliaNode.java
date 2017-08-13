package io.zulia.server.node;

import io.zulia.server.config.NodeService;
import io.zulia.server.config.ZuliaConfig;
import io.zulia.server.connection.server.ZuliaServiceServer;
import io.zulia.server.index.ZuliaIndexManager;
import io.zulia.server.rest.ZuliaRESTServiceManager;

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

		MembershipTask membershipTask = new MembershipTask(zuliaConfig, nodeService) {

			@Override
			protected void handleNodeRemove(Collection<Node> currentOtherNodesActive, Node removedNode) {
				indexManager.handleNodeRemoved(currentOtherNodesActive, removedNode);
			}

			@Override
			protected void handleNodeAdded(Collection<Node> currentOtherNodesActive, Node newNode) {
				indexManager.handleNodeAdded(currentOtherNodesActive, newNode);
			}
		};
		//force membership to run before this constructor exits
		membershipTask.run();

		membershipTimer.scheduleAtFixedRate(membershipTask, 1000, 1000);

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

	public void start() throws Exception {
		indexManager.init();
		restServiceManager.start();
		zuliaServiceServer.start();

	}

	public static boolean isEqual(Node node1, Node node2) {
		return (node1.getServerAddress().equals(node2.getServerAddress()) && node1.getServicePort() == node2.getServicePort());
	}

	public static Node nodeFromConfig(ZuliaConfig zuliaConfig) {
		return Node.newBuilder().setServerAddress(zuliaConfig.getServerAddress()).setServicePort(zuliaConfig.getServicePort())
				.setRestPort(zuliaConfig.getRestPort()).build();
	}

}
