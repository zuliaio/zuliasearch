package io.zulia.server;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import io.zulia.server.config.IndexConfig;
import io.zulia.server.config.MongoNodeConfig;
import io.zulia.server.config.MongoServer;
import io.zulia.server.config.NodeConfig;
import io.zulia.server.config.ZuliaConfig;
import io.zulia.server.util.ServerNameHelper;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.search.BooleanQuery;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static io.zulia.message.ZuliaBase.Node;

public class ZuliaD {

	private static final Gson GSON = new GsonBuilder().create();

	public static class ZuliaArgs {

		@Parameter(names = "--help", help = true)
		private boolean help;

		@Parameter(names = "--config", description = "Full path to the config (defaults to $APP_HOME/bin/zulia.properties)")
		private String configPath = "bin" + File.separator + "zulia.properties";
	}

	public static class StartArgs {

	}

	public static class AddNodeArgs {

	}

	public static class RemoveNodeArgs {

		@Parameter(names = "--server", description = "Server to remove from cluster", required = true)
		private String server;

		@Parameter(names = "--hazelcastPort", description = "Hazelcast port of server to remove from cluster", required = true)
		private int hazelcastPort;

	}

	public static void main(String[] args) {

		ZuliaArgs zuliaArgs = new ZuliaArgs();
		StartArgs startArgs = new StartArgs();
		AddNodeArgs addNodeArgs = new AddNodeArgs();
		RemoveNodeArgs removeNodeArgs = new RemoveNodeArgs();

		JCommander jCommander = JCommander.newBuilder().addObject(zuliaArgs).addCommand("start", startArgs).addCommand("addNode", addNodeArgs)
				.addCommand("removeNode", removeNodeArgs).build();
		try {
			jCommander.parse(args);

			String prefix = System.getenv("APP_HOME");

			String config = zuliaArgs.configPath;
			if (prefix != null) {
				config = prefix + File.separator + config;
			}

			ZuliaConfig zuliaConfig = GSON.fromJson(new FileReader(config), ZuliaConfig.class);

			String dataDir = zuliaConfig.getDataPath();
			Path dataPath = Paths.get(dataDir);

			IndexConfig indexConfig = null;
			NodeConfig nodeConfig = null;

			File dataFile = dataPath.toFile();
			if (!dataFile.exists()) {
				throw new IOException("Data dir <" + dataDir + "> does not exist");
			}

			if (zuliaConfig.getServerAddress() == null) {
				zuliaConfig.setServerAddress(ServerNameHelper.getLocalServer());
			}

			if (zuliaConfig.isCluster()) {
				List<MongoServer> mongoServers = zuliaConfig.getMongoServers();

				List<ServerAddress> serverAddressList = new ArrayList<>();

				for (MongoServer mongoServer : mongoServers) {
					serverAddressList.add(new ServerAddress(mongoServer.getHostname(), mongoServer.getPort()));
				}

				MongoProvider.setMongoClient(new MongoClient(serverAddressList));

				//indexConfig = new MongoIndexConfig(mongoServers);
				nodeConfig = new MongoNodeConfig(MongoProvider.getMongoClient(), zuliaConfig.getClusterName());
			}
			else {
				//indexConfig = new FileIndexConfig(dataPath + File.separator + "config");
				//nodeConfig = new FileNodeConfig(dataPath + File.separator + "config");
			}

			if ("start".equals(jCommander.getParsedCommand())) {
				setLuceneStatic();
				List<Node> nodes = nodeConfig.getNodes();

			}
			else if ("addNode".equals(jCommander.getParsedCommand())) {
				if (!zuliaConfig.isCluster()) {
					throw new IllegalArgumentException("Add node is only available in cluster mode");
				}

				Node node = Node.newBuilder().setServerAddress(zuliaConfig.getServerAddress()).setHazelcastPort(zuliaConfig.getHazelcastPort())
						.setServicePort(zuliaConfig.getServicePort()).setRestPort(zuliaConfig.getRestPort()).build();

				nodeConfig.addNode(node);

			}
			else if ("removeNode".equals(jCommander.getParsedCommand())) {
				if (!zuliaConfig.isCluster()) {
					throw new IllegalArgumentException("Add node is only available in cluster mode");
				}

				Node node = Node.newBuilder().setServerAddress(removeNodeArgs.server).setHazelcastPort(removeNodeArgs.hazelcastPort).build();
				nodeConfig.removeNode(node);
			}

		}
		catch (ParameterException e) {
			jCommander.usage();
			System.exit(2);
		}
		catch (Exception e) {
			System.err.println("Error: " + e.getMessage());
			System.exit(1);
		}

		System.out.println(zuliaArgs.configPath);

	}

	private static void setLuceneStatic() {
		BooleanQuery.setMaxClauseCount(16 * 1024);
		FacetsConfig.DEFAULT_DIM_CONFIG.multiValued = true;
	}
}
