package io.zulia.server.cmd;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.zulia.log.LogUtil;
import io.zulia.server.config.NodeService;
import io.zulia.server.config.ZuliaConfig;
import io.zulia.server.config.cluster.MongoAuth;
import io.zulia.server.config.cluster.MongoNodeService;
import io.zulia.server.config.cluster.MongoServer;
import io.zulia.server.config.single.SingleNodeService;
import io.zulia.server.node.ZuliaNode;
import io.zulia.server.util.MongoProvider;
import io.zulia.server.util.ServerNameHelper;
import io.zulia.server.util.ZuliaNodeProvider;
import io.zulia.util.ZuliaVersion;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.search.BooleanQuery;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.logging.Logger;

import static io.zulia.message.ZuliaBase.Node;

public class ZuliaD {

	private static final Logger LOG = Logger.getLogger(ZuliaD.class.getSimpleName());

	private static final Gson GSON = new GsonBuilder().create();

	public static class ZuliaDArgs {

		@Parameter(names = "--help", help = true)
		private boolean help;

		@Parameter(names = "--config", description = "Full path to the config (defaults to $APP_HOME/config/zulia.properties)")
		private String configPath = "config" + File.separator + "zulia.properties";
	}

	@Parameters
	public static class StartArgs {

	}

	@Parameters
	public static class ListNodesArgs {

	}

	@Parameters
	public static class AddNodeArgs {
		@Parameter(names = "--server", description = "Server to add to the cluster", required = false)
		private String server;

		@Parameter(names = "--servicePort", description = "Service port of server to add to cluster", required = false)
		private int servicePort;
	}

	@Parameters
	public static class RemoveNodeArgs {

		@Parameter(names = "--server", description = "Server to remove from cluster", required = true)
		private String server;

		@Parameter(names = "--servicePort", description = "Service port of server to remove from cluster", required = true)
		private int servicePort;

	}

	public static void main(String[] args) {

		LogUtil.init();

		ZuliaDArgs zuliaDArgs = new ZuliaDArgs();
		StartArgs startArgs = new StartArgs();
		AddNodeArgs addNodeArgs = new AddNodeArgs();
		RemoveNodeArgs removeNodeArgs = new RemoveNodeArgs();
		ListNodesArgs listNodesArgs = new ListNodesArgs();

		JCommander jCommander = JCommander.newBuilder().addObject(zuliaDArgs).addCommand("start", startArgs).addCommand("addNode", addNodeArgs)
				.addCommand("removeNode", removeNodeArgs).addCommand("listNodes", listNodesArgs).build();
		try {
			jCommander.parse(args);

			String parsedCommand = jCommander.getParsedCommand();
			if (parsedCommand == null) {
				jCommander.usage();
				System.exit(2);
			}

			String prefix = System.getenv("APP_HOME");

			String config = zuliaDArgs.configPath;
			if (prefix != null && !config.startsWith(File.separator)) {
				config = prefix + File.separator + config;
			}

			LOG.info("Path: " + config);

			ZuliaConfig zuliaConfig = GSON.fromJson(new FileReader(config), ZuliaConfig.class);
			LOG.info("Using config <" + config + ">");

			String dataDir = zuliaConfig.getDataPath();
			if (prefix != null && !dataDir.startsWith(File.separator)) {
				dataDir = prefix + File.separator + dataDir;
				zuliaConfig.setDataPath(dataDir);
			}
			Path dataPath = Paths.get(dataDir);

			NodeService nodeService;

			File dataFile = dataPath.toFile();
			if (!dataFile.exists()) {
				throw new IOException("Data dir <" + dataDir + "> does not exist");
			}
			else {
				LOG.info("Using data directory <" + dataFile.getAbsolutePath() + ">");
			}

			if (zuliaConfig.getServerAddress() == null) {
				zuliaConfig.setServerAddress(ServerNameHelper.getLocalServer());
			}

			if (zuliaConfig.isCluster()) {
				List<MongoServer> mongoServers = zuliaConfig.getMongoServers();

				List<ServerAddress> serverAddressList = new ArrayList<>();

				for (MongoServer mongoServer : mongoServers) {
					LOG.info("Added Mongo Server: " + mongoServer);
					serverAddressList.add(new ServerAddress(mongoServer.getHostname(), mongoServer.getPort()));
				}

				MongoClientSettings.Builder mongoBuilder = MongoClientSettings.builder().applyToClusterSettings(builder -> builder.hosts(serverAddressList));

				MongoAuth mongoAuth = zuliaConfig.getMongoAuth();
				if (mongoAuth != null) {
					mongoBuilder.credential(
							MongoCredential.createCredential(mongoAuth.getUsername(), mongoAuth.getDatabase(), mongoAuth.getPassword().toCharArray()));
				}

				MongoClient mongoClient = MongoClients.create(mongoBuilder.build());

				MongoProvider.setMongoClient(mongoClient);
				LOG.info("Created Mongo Client: " + MongoProvider.getMongoClient());

				nodeService = new MongoNodeService(MongoProvider.getMongoClient(), zuliaConfig.getClusterName());
				LOG.info("Created Mongo Node Service");
			}
			else {
				nodeService = new SingleNodeService(zuliaConfig);
				LOG.info("Created Single Node Service");
			}

			if ("start".equals(parsedCommand)) {
				setLuceneStatic();

				Collection<Node> nodes = nodeService.getNodes();

				if (zuliaConfig.isCluster()) {
					if (nodes.isEmpty()) {
						LOG.severe("No nodes added to the cluster");
						System.exit(3);
					}
				}
				else {
					LOG.warning("Running in single node mode");
				}

				displayNodes(nodeService, "Registered nodes:");

				ZuliaNode zuliaNode = new ZuliaNode(zuliaConfig, nodeService);

				ZuliaNodeProvider.setZuliaNode(zuliaNode);
				zuliaNode.start();
			}
			else if ("addNode".equals(parsedCommand)) {
				Node node = Node.newBuilder().setServerAddress(zuliaConfig.getServerAddress()).setServicePort(zuliaConfig.getServicePort())
						.setRestPort(zuliaConfig.getRestPort()).setVersion(ZuliaVersion.getVersion()).build();

				LOG.info("Adding node: " + formatNode(node));

				nodeService.addNode(node);

				displayNodes(nodeService, "Registered Nodes:");

			}
			else if ("removeNode".equals(parsedCommand)) {

				LOG.info("Removing node: " + removeNodeArgs.server + ":" + removeNodeArgs.servicePort);
				nodeService.removeNode(removeNodeArgs.server, removeNodeArgs.servicePort);

				displayNodes(nodeService, "Registered Nodes:");
			}
			else if ("listNodes".equals(parsedCommand)) {
				displayNodes(nodeService, "Registered Nodes:");
			}

		}
		catch (ParameterException e) {
			jCommander.usage();
			System.exit(2);
		}
		catch (UnsupportedOperationException e) {
			System.err.println("Error: " + e.getMessage());
			System.exit(2);
		}
		catch (Exception e) {
			System.err.println("Error: " + e.getMessage());
			e.printStackTrace();
			System.exit(1);
		}

	}

	private static void displayNodes(NodeService nodeService, String header) throws InvalidProtocolBufferException {
		LOG.info(header);
		for (Node node : nodeService.getNodes()) {
			LOG.info("  " + formatNode(node));
		}
	}

	private static String formatNode(Node node) throws InvalidProtocolBufferException {
		JsonFormat.Printer printer = JsonFormat.printer();
		return printer.print(node).replace("\n", " ").replaceAll("\\s+", " ");
	}

	public static void setLuceneStatic() {
		BooleanQuery.setMaxClauseCount(128 * 1024);
		FacetsConfig.DEFAULT_DIM_CONFIG.multiValued = true;
	}
}
