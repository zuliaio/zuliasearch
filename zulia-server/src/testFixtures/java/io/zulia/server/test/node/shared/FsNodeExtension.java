package io.zulia.server.test.node.shared;

import io.zulia.client.config.ZuliaPoolConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.message.ZuliaBase;
import io.zulia.server.config.ZuliaConfig;
import io.zulia.server.config.single.SingleNodeService;
import io.zulia.server.node.ZuliaNode;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.function.Consumer;

/**
 * Starts a single Zulia node in filesystem mode (cluster false): FSIndexService for index metadata,
 * FileDocumentStorage for associated documents, and no MongoDB. Complements NodeExtension, which
 * only covers cluster mode. Data persists across {@link #restartNode()} within one suite and is
 * wiped before the suite starts. Each node owns {@code basePath/node-fs-<servicePort>} and only that
 * directory is wiped, so suites that may run in parallel (other modules under a parallel build) just
 * pick their own ports and can share the base path.
 */
public class FsNodeExtension implements BeforeAllCallback, AfterAllCallback {

	private final static Logger LOG = LoggerFactory.getLogger(FsNodeExtension.class);

	public static final String DEFAULT_BASE_PATH = "/tmp/zuliaTestFs";
	public static final int DEFAULT_SERVICE_PORT = 21191;
	public static final int DEFAULT_REST_PORT = 21192;

	private final String dataPath;
	private final int servicePort;
	private final int restPort;
	private final Consumer<ZuliaConfig> configCustomizer;

	private ZuliaNode zuliaNode;
	private ZuliaWorkPool zuliaWorkPool;

	public FsNodeExtension() {
		this(null);
	}

	public FsNodeExtension(Consumer<ZuliaConfig> configCustomizer) {
		this(DEFAULT_SERVICE_PORT, DEFAULT_REST_PORT, DEFAULT_BASE_PATH, configCustomizer);
	}

	public FsNodeExtension(int servicePort, int restPort, Consumer<ZuliaConfig> configCustomizer) {
		this(servicePort, restPort, DEFAULT_BASE_PATH, configCustomizer);
	}

	public FsNodeExtension(int servicePort, int restPort, String basePath, Consumer<ZuliaConfig> configCustomizer) {
		this.servicePort = servicePort;
		this.restPort = restPort;
		this.dataPath = basePath + "/node-fs-" + servicePort;
		this.configCustomizer = configCustomizer;
	}

	public ZuliaWorkPool getClient() {
		return zuliaWorkPool;
	}

	public ZuliaNode getNode() {
		return zuliaNode;
	}

	public String getDataPath() {
		return dataPath;
	}

	@Override
	public void beforeAll(ExtensionContext context) throws Exception {
		LOG.info("FS mode suite started: {}", context.getTestClass().orElse(null));
		deleteNodeDir();
		Files.createDirectories(Path.of(dataPath));
		startNode();
		Thread.sleep(2000);

		ZuliaPoolConfig zuliaPoolConfig = new ZuliaPoolConfig();
		zuliaPoolConfig.addNode(ZuliaBase.Node.newBuilder().setServerAddress("localhost").setServicePort(servicePort).setRestPort(restPort).build());
		zuliaWorkPool = new ZuliaWorkPool(zuliaPoolConfig);
	}

	@Override
	public void afterAll(ExtensionContext context) {
		if (zuliaNode != null) {
			zuliaNode.shutdown();
		}
		if (zuliaWorkPool != null) {
			zuliaWorkPool.close();
		}
		LOG.info("FS mode suite finished: {}", context.getTestClass().orElse(null));
	}

	public void restartNode() throws Exception {
		zuliaNode.shutdown();
		Thread.sleep(2000);
		startNode();
		Thread.sleep(2000);
	}

	private void startNode() throws Exception {
		ZuliaConfig zuliaConfig = new ZuliaConfig();
		zuliaConfig.setServerAddress("localhost");
		zuliaConfig.setCluster(false);
		zuliaConfig.setDataPath(dataPath);
		zuliaConfig.setServicePort(servicePort);
		zuliaConfig.setRestPort(restPort);
		if (configCustomizer != null) {
			configCustomizer.accept(zuliaConfig);
		}
		zuliaNode = new ZuliaNode(zuliaConfig, new SingleNodeService(zuliaConfig));
		zuliaNode.start(false);
	}

	// Only this node's directory, so another suite's node under the same base path is untouched.
	private void deleteNodeDir() throws IOException {
		Path nodeDir = Path.of(dataPath);
		if (Files.exists(nodeDir)) {
			try (var paths = Files.walk(nodeDir)) {
				paths.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
			}
		}
	}
}
