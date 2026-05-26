package io.zulia.server.node;

import io.micronaut.context.ApplicationContext;
import io.micronaut.context.annotation.Bean;
import io.micronaut.runtime.Micronaut;
import io.zulia.server.config.NodeService;
import io.zulia.server.config.ZuliaConfig;
import io.zulia.server.connection.server.ZuliaServiceServer;
import io.zulia.server.index.ZuliaIndexManager;
import io.zulia.server.rest.ZuliaRESTService;
import io.zulia.util.ZuliaVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Timer;

import static io.zulia.message.ZuliaBase.Node;

@Bean
public class ZuliaNode {

	private static final Logger LOG = LoggerFactory.getLogger(ZuliaNode.class);
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

		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			if (started) {
				shutdown();
			}
		}));

	}

	public static boolean isEqual(Node node1, Node node2) {
		return (node1.getServerAddress().equals(node2.getServerAddress()) && node1.getServicePort() == node2.getServicePort());
	}

	public static Node nodeFromConfig(ZuliaConfig zuliaConfig) {
		return Node.newBuilder().setServerAddress(zuliaConfig.getServerAddress()).setServicePort(zuliaConfig.getServicePort())
				.setRestPort(zuliaConfig.getRestPort()).setVersion(ZuliaVersion.getVersion()).build();
	}

	public void start() throws Exception {
		start(true);
	}

	public void start(boolean startREST) throws Exception {
		LOG.info("{}:{} starting", zuliaConfig.getServerAddress(), zuliaConfig.getServicePort());
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
			Map<String, Object> properties = Map.of("micronaut.server.host", zuliaConfig.getServerAddress(), "micronaut.server.port",
					zuliaConfig.getRestPort());
			micronautService = Micronaut.build((String) null).mainClass(ZuliaRESTService.class).properties(properties).singletons(this).start();
		}
		LOG.info("{}:{} started", zuliaConfig.getServerAddress(), zuliaConfig.getServicePort());

		started = true;
	}

	public void shutdown() {
		LOG.info("{}:{} stopping", zuliaConfig.getServerAddress(), zuliaConfig.getServicePort());
		membershipTimer.cancel();
		nodeService.removeHeartbeat(zuliaConfig.getServerAddress(), zuliaConfig.getServicePort());
		zuliaServiceServer.shutdown();
		indexManager.shutdown();
		if (micronautService != null) {
			try {
				// Stop on a separate thread to preserve the context classloader, but wait for it to finish:
				// until Micronaut actually stops, the REST port stays bound, and a fast restart on the same
				// port (e.g. back-to-back tests) fails with "address already in use".
				Thread thread = new Thread(this::stopMicronautServer);
				thread.setContextClassLoader(getClass().getClassLoader());
				thread.start();
				thread.join(15_000L);
				if (thread.isAlive()) {
					LOG.warn("Micronaut REST server did not stop within 15s for {}:{}", zuliaConfig.getServerAddress(), zuliaConfig.getRestPort());
				}
			}
			catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			catch (Exception e) {
				LOG.error("Failed to stop Micronaut", e);
			}
		}

		LOG.info("{}:{} stopped", zuliaConfig.getServerAddress(), zuliaConfig.getServicePort());

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

	public ZuliaIndexManager getIndexManager() {
		return indexManager;
	}

	public ZuliaConfig getZuliaConfig() {
		return zuliaConfig;
	}
}
