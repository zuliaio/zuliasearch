package io.zulia.server.node;

import io.micronaut.context.ApplicationContext;
import io.micronaut.core.util.CollectionUtils;
import io.micronaut.runtime.Micronaut;
import io.zulia.server.config.NodeService;
import io.zulia.server.config.ZuliaConfig;
import io.zulia.server.connection.server.ZuliaServiceServer;
import io.zulia.server.index.ZuliaIndexManager;
import io.zulia.server.rest.ZuliaRESTService;

import java.util.Collection;
import java.util.Map;
import java.util.Timer;
import java.util.logging.Level;
import java.util.logging.Logger;

import static io.zulia.message.ZuliaBase.Node;

public class ZuliaNode {

	private static final Logger LOG = Logger.getLogger(ZuliaNode.class.getName());
	private final ZuliaIndexManager indexManager;
	private final ZuliaServiceServer zuliaServiceServer;
	private final Timer membershipTimer;
	private final NodeService nodeService;
	private final ZuliaConfig zuliaConfig;
	private ApplicationContext micronautService;

	private boolean started;

	public ZuliaNode(ZuliaConfig zuliaConfig, NodeService nodeService) throws Exception {

		this.zuliaConfig = zuliaConfig;
		this.nodeService = nodeService;
		this.indexManager = new ZuliaIndexManager(zuliaConfig, nodeService);

		this.zuliaServiceServer = new ZuliaServiceServer(zuliaConfig, indexManager);

		membershipTimer = new Timer();

		Runtime.getRuntime().addShutdownHook(new Thread() {
			@Override
			public void run() {
				if (started) {
					shutdown();
				}
			}
		});

	}

	public static boolean isEqual(Node node1, Node node2) {
		return (node1.getServerAddress().equals(node2.getServerAddress()) && node1.getServicePort() == node2.getServicePort());
	}

	public static Node nodeFromConfig(ZuliaConfig zuliaConfig) {
		return Node.newBuilder().setServerAddress(zuliaConfig.getServerAddress()).setServicePort(zuliaConfig.getServicePort())
				.setRestPort(zuliaConfig.getRestPort()).build();
	}

	public void start() throws Exception {
		start(true);
	}

	public void start(boolean startREST) throws Exception {
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
		if (startREST) {
			Map<String, Object> properties = CollectionUtils.mapOf("micronaut.server.host", zuliaConfig.getServerAddress(), "micronaut.server.port",
					zuliaConfig.getRestPort());
			micronautService = Micronaut.build((String) null).mainClass(ZuliaRESTService.class).properties(properties).start();
		}
		LOG.info(getLogPrefix() + "started");

		started = true;
	}

	public void shutdown() {
		LOG.info(getLogPrefix() + "stopping");
		membershipTimer.cancel();
		nodeService.removeHeartbeat(zuliaConfig.getServerAddress(), zuliaConfig.getServicePort());
		zuliaServiceServer.shutdown();
		indexManager.shutdown();
		if (micronautService != null) {
			try {
				Thread thread = new Thread(this::stopMicronautServer);
				thread.setContextClassLoader(getClass().getClassLoader());
				thread.start();
			}
			catch (Exception e) {
				LOG.log(Level.SEVERE, "Failed to stop Micronaut", e);
			}
		}

		LOG.info(getLogPrefix() + "stopped");

		started = false;
	}

	private void stopMicronautServer() {
		try {
			Thread.sleep(500L);
		}
		catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
		}
		this.micronautService.stop();
	}

	private String getLogPrefix() {
		return zuliaConfig.getServerAddress() + ":" + zuliaConfig.getServicePort() + " ";
	}

	public ZuliaIndexManager getIndexManager() {
		return indexManager;
	}

	public ZuliaConfig getZuliaConfig() {
		return zuliaConfig;
	}
}
