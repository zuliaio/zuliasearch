package io.zulia.server.test.node;

import com.mongodb.MongoClient;
import io.zulia.message.ZuliaBase;
import io.zulia.server.cmd.ZuliaD;
import io.zulia.server.config.ZuliaConfig;
import io.zulia.server.config.cluster.MongoNodeService;
import io.zulia.server.config.cluster.MongoServer;
import io.zulia.server.node.ZuliaNode;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

public class TestHelper {

	public static final String MONGO_SERVER_PROPERTY = "mongoServer";
	public static final String TEST_CLUSTER_NAME = "zuliaTest";

	public static final String MONGO_SERVER_PROPERTY_DEFAULT = "127.0.0.1";

	private static final MongoClient mongoClient;

	static {
		String mongoServer = getMongoServer();
		mongoClient = new MongoClient(mongoServer);
	}

	private List<ZuliaNode> zuliaNodes = new ArrayList<>();

	private static String getMongoServer() {
		String mongoServer = System.getProperty(MONGO_SERVER_PROPERTY);
		if (mongoServer == null) {
			mongoServer = MONGO_SERVER_PROPERTY_DEFAULT;
		}
		return mongoServer;
	}

	public static MongoClient getMongoClient() {
		return mongoClient;
	}

	public void startSuite(int instanceCount) throws Exception {
		ZuliaD.setLuceneStatic();

	}

	public void startNodes(int nodeCount) throws Exception {

		Path dataPath = Paths.get("/tmp/zuliaTest");

		Files.walk(dataPath).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);

		mongoClient.getDatabase(TEST_CLUSTER_NAME).drop();

		MongoNodeService nodeService = new MongoNodeService(mongoClient, TEST_CLUSTER_NAME);

		int port = 20000;

		for (int i = 0; i < nodeCount; i++) {
			ZuliaBase.Node node = ZuliaBase.Node.newBuilder().setServerAddress("localhost").setServicePort(++port).setRestPort(++port).build();
			nodeService.addNode(node);
		}

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
			zuliaNode.start();

			zuliaNodes.add(zuliaNode);
		}

	}

	public void stopNodes() throws Exception {

		for (ZuliaNode zuliaNode : zuliaNodes) {
			zuliaNode.shutdown();
		}

	}

}
