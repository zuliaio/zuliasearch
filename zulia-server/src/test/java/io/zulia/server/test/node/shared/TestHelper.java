package io.zulia.server.test.node.shared;

import com.mongodb.client.MongoClients;
import io.zulia.client.config.ZuliaPoolConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.rest.ZuliaRESTClient;
import io.zulia.message.ZuliaBase;
import io.zulia.server.cmd.zuliad.ZuliaDConfig;
import io.zulia.server.config.ZuliaConfig;
import io.zulia.server.config.cluster.MongoNodeService;
import io.zulia.server.config.cluster.MongoServer;
import io.zulia.server.node.ZuliaNode;
import io.zulia.server.test.mongo.MongoTestInstance;
import io.zulia.server.util.MongoProvider;
import io.zulia.server.util.ZuliaNodeProvider;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class TestHelper {
	private final static Logger LOG = LoggerFactory.getLogger(TestHelper.class);
	private static final String MONGO_TEST_CONNECTION = "mongoTestConnection";
	private static final String TEST_CLUSTER_NAME = "zuliaTest";
	private static final String MONGO_TEST_CONNECTION_DEFAULT = "mongodb://127.0.0.1:27017";
	private static final Pattern MONGO_URL_PATTERN = Pattern.compile("([^:]+)://([^:]+):(\\d+)");
	private static final MongoNodeService NODE_SERVICE;
	private static final MongoTestInstance MONGO_TEST_INSTANCE;
	private static final List<ZuliaNode> ZULIA_NODES = new ArrayList<>();

	static {

		ZuliaDConfig.setLuceneStatic();

		MONGO_TEST_INSTANCE = new MongoTestInstance();

		if (isInMemoryMongoTestInstanceRequired()) {
			MONGO_TEST_INSTANCE.start();
			Runtime.getRuntime().addShutdownHook(new Thread(TestHelper::shutdownTestMongoInstance));
		}

		String mongoServer = getMongoServer();

		MongoProvider.setMongoClient(MongoClients.create(mongoServer));

		MongoProvider.getMongoClient().getDatabase(TEST_CLUSTER_NAME).drop();

		NODE_SERVICE = new MongoNodeService(MongoProvider.getMongoClient(), TEST_CLUSTER_NAME);

		clearData();

	}

	private static void clearData() {
		try {
			Path dataPath = Paths.get("/tmp/zuliaTest");

			if (Files.exists(dataPath)) {
				try (Stream<Path> walk = Files.walk(dataPath)) {
					walk.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
				}
			}
			Files.createDirectory(dataPath);
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	private static String getMongoServer() {

		String mongoServer;

		if (isInMemoryMongoTestInstanceRequired()) {

			mongoServer = MONGO_TEST_INSTANCE.getInstanceUrl();

		}
		else {

			mongoServer = System.getProperty(MONGO_TEST_CONNECTION);

			if (StringUtils.isEmpty(mongoServer)) {
				mongoServer = MONGO_TEST_CONNECTION_DEFAULT;
			}
		}

		return mongoServer;
	}

	public static ZuliaRESTClient createRESTClient() {
		for (ZuliaNode zuliaNode : ZULIA_NODES) {
			ZuliaNodeProvider.setZuliaNode(zuliaNode);
			return new ZuliaRESTClient("http://" + zuliaNode.getZuliaConfig().getServerAddress() + ":" + zuliaNode.getZuliaConfig().getRestPort());
		}
		throw new RuntimeException("No nodes are defined, ");
	}

	public static ZuliaWorkPool createClient() {

		ZuliaPoolConfig zuliaPoolConfig = new ZuliaPoolConfig();
		for (ZuliaBase.Node node : NODE_SERVICE.getNodes()) {
			zuliaPoolConfig.addNode(node);
		}

		return new ZuliaWorkPool(zuliaPoolConfig);

	}

	public static void startNodes(boolean startRest) throws Exception {
		LOG.info("Starting <{}> Nodes", NODE_SERVICE.getNodes().size());
		int i = 0;
		for (ZuliaBase.Node node : NODE_SERVICE.getNodes()) {
			ZuliaConfig zuliaConfig = new ZuliaConfig();
			zuliaConfig.setServerAddress("localhost");
			zuliaConfig.setCluster(true);
			zuliaConfig.setClusterName(TEST_CLUSTER_NAME);
			zuliaConfig.setResponseCompression(true);
			//zuliaConfig.setRpcWorkers(44);

			String mongoServerUrl = getMongoServer();
			zuliaConfig.setMongoServers(Collections.singletonList(new MongoServer(mongoServerUrl, parseMongoPort(mongoServerUrl))));

			zuliaConfig.setDataPath("/tmp/zuliaTest/node" + i);
			zuliaConfig.setRestPort(node.getRestPort());
			zuliaConfig.setServicePort(node.getServicePort());
			i++;

			ZuliaNode zuliaNode = new ZuliaNode(zuliaConfig, NODE_SERVICE);
			zuliaNode.start(startRest);

			ZULIA_NODES.add(zuliaNode);
		}
		LOG.info("Started <{}> Nodes", ZULIA_NODES.size());

	}

	public static void createNodes(int nodeCount) {
		clearData();
		LOG.info("Creating <{}> Nodes", nodeCount);
		int port = 20000;

		//drop nodes and index configs
		MongoProvider.getMongoClient().getDatabase(TEST_CLUSTER_NAME).drop();

		for (int i = 0; i < nodeCount; i++) {
			ZuliaBase.Node node = ZuliaBase.Node.newBuilder().setServerAddress("localhost").setServicePort(++port).setRestPort(++port).build();
			NODE_SERVICE.addNode(node);
		}
	}

	public static void stopNodes() {

		LOG.info("Stopping <{}> Nodes", ZULIA_NODES.size());
		for (ZuliaNode zuliaNode : ZULIA_NODES) {
			zuliaNode.shutdown();
		}
		ZULIA_NODES.clear();

	}

	protected static void shutdownTestMongoInstance() {
		MongoProvider.getMongoClient().close();
		MONGO_TEST_INSTANCE.shutdown();
	}

	private static boolean isInMemoryMongoTestInstanceRequired() {
		return StringUtils.isEmpty(System.getProperty(MONGO_TEST_CONNECTION));
	}

	private static Integer parseMongoPort(String mongoInstanceUrl) {
		Matcher matcher = MONGO_URL_PATTERN.matcher(mongoInstanceUrl);

		if (matcher.find()) {

			return Integer.valueOf(matcher.group(3));

		}
		else {
			throw new IllegalArgumentException("A Mongo Instance URL was provided with an invalid format: " + mongoInstanceUrl);
		}
	}

}
