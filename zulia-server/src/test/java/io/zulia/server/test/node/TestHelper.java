package io.zulia.server.test.node;

import com.mongodb.client.MongoClients;
import io.zulia.client.config.ZuliaPoolConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.log.LogUtil;
import io.zulia.message.ZuliaBase;
import io.zulia.server.cmd.ZuliaD;
import io.zulia.server.config.ZuliaConfig;
import io.zulia.server.config.cluster.MongoNodeService;
import io.zulia.server.config.cluster.MongoServer;
import io.zulia.server.node.ZuliaNode;
import io.zulia.server.util.MongoProvider;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class TestHelper {

	public static final String MONGO_TEST_CONNECTION = "mongoTestConnection";
	public static final String TEST_CLUSTER_NAME = "zuliaTest";

	public static final String MONGO_TEST_CONNECTION_DEFAULT = "mongodb://127.0.0.1:27017";

	private static final MongoNodeService nodeService;

	static {

		LogUtil.init();
		ZuliaD.setLuceneStatic();

		String mongoServer = getMongoServer();

		MongoProvider.setMongoClient(MongoClients.create(mongoServer));

		MongoProvider.getMongoClient().getDatabase(TEST_CLUSTER_NAME).drop();

		nodeService = new MongoNodeService(MongoProvider.getMongoClient(), TEST_CLUSTER_NAME);

		try {
			Path dataPath = Paths.get("/tmp/zuliaTest");

			if (Files.exists(dataPath)) {
				Files.walk(dataPath).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
			}
			Files.createDirectory(dataPath);
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}

	}

	private static List<ZuliaNode> zuliaNodes = new ArrayList<>();

	private static String getMongoServer() {

		String mongoServer = System.getProperty(MONGO_TEST_CONNECTION);
		if (mongoServer == null) {
			mongoServer = MONGO_TEST_CONNECTION_DEFAULT;
			System.out.println(mongoServer);
		}

		return mongoServer;
	}

	public static ZuliaWorkPool createClient() throws Exception {

		ZuliaPoolConfig zuliaPoolConfig = new ZuliaPoolConfig();
		for (ZuliaBase.Node node : nodeService.getNodes()) {
			System.out.println("Adding node: " + node.getServerAddress());
			zuliaPoolConfig.addNode(node);
		}

		System.out.println("Returning?");
		return new ZuliaWorkPool(zuliaPoolConfig);

	}

	public static void startNodes() throws Exception {

		int i = 0;
		for (ZuliaBase.Node node : nodeService.getNodes()) {
			ZuliaConfig zuliaConfig = new ZuliaConfig();
			zuliaConfig.setServerAddress("localhost");
			zuliaConfig.setCluster(true);
			zuliaConfig.setClusterName(TEST_CLUSTER_NAME);
			zuliaConfig.setMongoServers(Collections.singletonList(new MongoServer(getMongoServer(), 27017)));
			zuliaConfig.setDataPath("/tmp/zuliaTest/node" + i);
			zuliaConfig.setRestPort(node.getRestPort());
			zuliaConfig.setServicePort(node.getServicePort());
			i++;

			ZuliaNode zuliaNode = new ZuliaNode(zuliaConfig, nodeService);
			zuliaNode.start(false);

			zuliaNodes.add(zuliaNode);
		}

	}

	public static void createNodes(int nodeCount) {
		int port = 20000;

		for (int i = 0; i < nodeCount; i++) {
			ZuliaBase.Node node = ZuliaBase.Node.newBuilder().setServerAddress("localhost").setServicePort(++port).setRestPort(++port).build();
			nodeService.addNode(node);
		}
	}

	public static void stopNodes() throws Exception {

		for (ZuliaNode zuliaNode : zuliaNodes) {
			zuliaNode.shutdown();
		}

	}

}
