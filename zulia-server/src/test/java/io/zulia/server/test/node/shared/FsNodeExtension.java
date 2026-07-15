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
 * FileDocumentStorage for associated documents, and no MongoDB. Complements {@link NodeExtension},
 * which only covers cluster mode. Data persists across {@link #restartNode()} within one suite and is
 * wiped before the suite starts.
 */
public class FsNodeExtension implements BeforeAllCallback, AfterAllCallback {

	private final static Logger LOG = LoggerFactory.getLogger(FsNodeExtension.class);

	private static final String BASE_PATH = "/tmp/zuliaTestFs";
	private static final String DATA_PATH = BASE_PATH + "/node0";
	private static final int SERVICE_PORT = 21191;
	private static final int REST_PORT = 21192;

	private final Consumer<ZuliaConfig> configCustomizer;

	private ZuliaNode zuliaNode;
	private ZuliaWorkPool zuliaWorkPool;

	public FsNodeExtension() {
		this(null);
	}

	public FsNodeExtension(Consumer<ZuliaConfig> configCustomizer) {
		this.configCustomizer = configCustomizer;
	}

	public ZuliaWorkPool getClient() {
		return zuliaWorkPool;
	}

	public ZuliaNode getNode() {
		return zuliaNode;
	}

	public String getDataPath() {
		return DATA_PATH;
	}

	@Override
	public void beforeAll(ExtensionContext context) throws Exception {
		LOG.info("FS mode suite started: {}", context.getTestClass().orElse(null));
		deleteBaseDir();
		Files.createDirectories(Path.of(DATA_PATH));
		startNode();
		Thread.sleep(2000);

		ZuliaPoolConfig zuliaPoolConfig = new ZuliaPoolConfig();
		zuliaPoolConfig.addNode(ZuliaBase.Node.newBuilder().setServerAddress("localhost").setServicePort(SERVICE_PORT).setRestPort(REST_PORT).build());
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
		zuliaConfig.setDataPath(DATA_PATH);
		zuliaConfig.setServicePort(SERVICE_PORT);
		zuliaConfig.setRestPort(REST_PORT);
		if (configCustomizer != null) {
			configCustomizer.accept(zuliaConfig);
		}
		zuliaNode = new ZuliaNode(zuliaConfig, new SingleNodeService(zuliaConfig));
		zuliaNode.start(false);
	}

	private void deleteBaseDir() throws IOException {
		Path basePath = Path.of(BASE_PATH);
		if (Files.exists(basePath)) {
			try (var paths = Files.walk(basePath)) {
				paths.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
			}
		}
	}
}
