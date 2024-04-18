package io.zulia.server.cmd.zuliad;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.ServerApi;
import com.mongodb.ServerApiVersion;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.zulia.cmd.common.ZuliaCommonCmd;
import io.zulia.message.ZuliaBase;
import io.zulia.server.config.NodeService;
import io.zulia.server.config.ZuliaConfig;
import io.zulia.server.config.cluster.MongoAuth;
import io.zulia.server.config.cluster.MongoNodeService;
import io.zulia.server.config.cluster.MongoServer;
import io.zulia.server.config.single.SingleNodeService;
import io.zulia.server.util.MongoProvider;
import io.zulia.server.util.ServerNameHelper;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.search.IndexSearcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.LoaderOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class ZuliaDConfig {

	private static final Gson GSON = new GsonBuilder().create();
	private static final Logger LOG = LoggerFactory.getLogger(ZuliaDConfig.class);
	private NodeService nodeService;
	private ZuliaConfig zuliaConfig;

	public ZuliaDConfig(String config) throws IOException {
		String prefix = System.getenv("APP_HOME");

		if (prefix != null && !config.startsWith(File.separator)) {
			config = prefix + File.separator + config;
		}

		LOG.info("Loading config <" + config + ">");

		try (FileReader fr = new FileReader(config)) {

			if (config.endsWith("json") || config.endsWith("properties")) {
				zuliaConfig = GSON.fromJson(fr, ZuliaConfig.class);
			}
			else if (config.endsWith("yml") || config.endsWith("yaml")) {
				Yaml yaml = new Yaml(new Constructor(ZuliaConfig.class, new LoaderOptions()));
				zuliaConfig = yaml.load(fr);
			}
			else {
				throw new RuntimeException("Incompatible config file provided: " + config);
			}
		}

		String dataDir = zuliaConfig.getDataPath();
		if (prefix != null && !dataDir.startsWith(File.separator)) {
			dataDir = prefix + File.separator + dataDir;
			zuliaConfig.setDataPath(dataDir);
		}
		Path dataPath = Paths.get(dataDir);

		File dataFile = dataPath.toFile();
		if (!dataFile.exists()) {
			throw new FileNotFoundException("Data dir <" + dataDir + "> does not exist");
		}
		else {
			LOG.info("Using data directory <" + dataFile.getAbsolutePath() + ">");
		}

		if (zuliaConfig.getServerAddress() == null) {
			zuliaConfig.setServerAddress(ServerNameHelper.getLocalServer());
			LOG.warn("Server address is not defined.  Autodetected server name <" + zuliaConfig.getServerAddress() + ">");
		}

		if (zuliaConfig.isCluster()) {
			if (null != zuliaConfig.getMongoConnection()) {
				StringBuilder cs = new StringBuilder().append(zuliaConfig.getMongoConnection().getProtocol());
				if (!zuliaConfig.getMongoConnection().getProtocol().endsWith("://"))
					cs.append("://");
				if (null != zuliaConfig.getMongoAuth()) {
					cs.append(zuliaConfig.getMongoAuth().getUsername()).append(":").append(zuliaConfig.getMongoAuth().getPassword()).append("@");
				}
				cs.append(zuliaConfig.getMongoConnection().getConnectionURL());
				if (!zuliaConfig.getMongoConnection().getConnectionURL().endsWith("/"))
					cs.append("/");
				cs.append("?retryWrites=").append(zuliaConfig.getMongoConnection().isRetryWrites());
				cs.append("&w=").append(zuliaConfig.getMongoConnection().getWriteConcern());

				MongoClientSettings.Builder mongoClientOptions = MongoClientSettings.builder();
				mongoClientOptions.applyConnectionString(new ConnectionString(cs.toString()));
				mongoClientOptions.serverApi(ServerApi.builder().version(ServerApiVersion.V1).build());

				MongoClient mongoClient = MongoClients.create(mongoClientOptions.build());
				MongoProvider.setMongoClient(mongoClient);
			}
			else {
				List<MongoServer> mongoServers = zuliaConfig.getMongoServers();
				List<ServerAddress> serverAddressList = new ArrayList<>();
				for (MongoServer mongoServer : mongoServers) {
					LOG.info("Using Mongo Server: " + mongoServer);
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
			}

			nodeService = new MongoNodeService(MongoProvider.getMongoClient(), zuliaConfig.getClusterName());
			LOG.info("Created Mongo Node Service");
		}
		else {
			nodeService = new SingleNodeService(zuliaConfig);
			LOG.info("Created Single Node Service");
		}
	}

	public static void displayNodes(NodeService nodeService, String header) throws InvalidProtocolBufferException {
		System.out.println();
		ZuliaCommonCmd.printBlue(header);
		for (ZuliaBase.Node node : nodeService.getNodes()) {
			System.out.println(formatNode(node));
		}
	}

	public static String formatNode(ZuliaBase.Node node) throws InvalidProtocolBufferException {
		JsonFormat.Printer printer = JsonFormat.printer().omittingInsignificantWhitespace();
		return printer.print(node);
	}

	public static void setLuceneStatic() {
		IndexSearcher.setMaxClauseCount(128 * 1024);
		FacetsConfig.DEFAULT_DIM_CONFIG.multiValued = true;
	}

	public NodeService getNodeService() {
		return nodeService;
	}

	public ZuliaConfig getZuliaConfig() {
		return zuliaConfig;
	}
}
