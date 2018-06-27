package io.zulia.server.node;

import io.zulia.server.config.NodeService;
import io.zulia.server.config.ZuliaConfig;
import io.zulia.server.connection.server.ZuliaServiceServer;
import io.zulia.server.index.ZuliaIndexManager;
import io.zulia.server.rest.ZuliaRESTServiceManager;

import java.util.Collection;
import java.util.Timer;
import java.util.logging.Logger;

import static io.zulia.message.ZuliaBase.Node;

public class ZuliaNode {

	private final ZuliaIndexManager indexManager;
	private final ZuliaRESTServiceManager restServiceManager;

	private final ZuliaServiceServer zuliaServiceServer;

	private final Timer membershipTimer;
	private final NodeService nodeService;
	private final ZuliaConfig zuliaConfig;

	private static final Logger LOG = Logger.getLogger(ZuliaNode.class.getName());

	public ZuliaNode(ZuliaConfig zuliaConfig, NodeService nodeService) throws Exception {

		this.zuliaConfig = zuliaConfig;
		this.nodeService = nodeService;
		this.indexManager = new ZuliaIndexManager(zuliaConfig, nodeService);
		this.restServiceManager = new ZuliaRESTServiceManager(zuliaConfig, indexManager);

		this.zuliaServiceServer = new ZuliaServiceServer(zuliaConfig, indexManager);

		membershipTimer = new Timer();

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				shutdown();
			}
		});

	}

	public void start() throws Exception {
		LOG.info(getLogPrefix() + "starting");
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
		//force membership to run
		membershipTask.run();

		membershipTimer.scheduleAtFixedRate(membershipTask, 1000, 1000);

		indexManager.init();
		zuliaServiceServer.start();
		LOG.info(getLogPrefix() + "started");

	}

	public void shutdown() {
		LOG.info(getLogPrefix() + "stopping");
		membershipTimer.cancel();
		nodeService.removeHeartbeat(zuliaConfig.getServerAddress(), zuliaConfig.getServicePort());
		zuliaServiceServer.shutdown();
		indexManager.shutdown();
		LOG.info(getLogPrefix() + "stopped");
	}

	public static boolean isEqual(Node node1, Node node2) {
		return (node1.getServerAddress().equals(node2.getServerAddress()) && node1.getServicePort() == node2.getServicePort());
	}

	public static Node nodeFromConfig(ZuliaConfig zuliaConfig) {
		return Node.newBuilder().setServerAddress(zuliaConfig.getServerAddress()).setServicePort(zuliaConfig.getServicePort())
				.setRestPort(zuliaConfig.getRestPort()).build();
	}

	private String getLogPrefix() {
		return zuliaConfig.getServerAddress() + ":" + zuliaConfig.getServicePort() + " ";
	}

}
