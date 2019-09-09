package io.zulia.server.cmd;

import com.mongodb.MongoClientSettings;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.zulia.server.config.NodeService;
import io.zulia.server.config.ZuliaConfig;
import io.zulia.server.config.cluster.MongoAuth;
import io.zulia.server.config.cluster.MongoNodeService;
import io.zulia.server.config.cluster.MongoServer;
import io.zulia.server.config.single.SingleNodeService;
import io.zulia.server.util.MongoProvider;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

public class ZuliaCmdUtil {

	private static final Logger LOG = Logger.getLogger(ZuliaCmdUtil.class.getSimpleName());

	public static NodeService getNodeService(ZuliaConfig zuliaConfig) {
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
				mongoBuilder
						.credential(MongoCredential.createCredential(mongoAuth.getUsername(), mongoAuth.getDatabase(), mongoAuth.getPassword().toCharArray()));
			}

			MongoClient mongoClient = MongoClients.create(mongoBuilder.build());

			MongoProvider.setMongoClient(mongoClient);
			LOG.info("Created Mongo Client: " + MongoProvider.getMongoClient());

			return new MongoNodeService(MongoProvider.getMongoClient(), zuliaConfig.getClusterName());
		}
		else {
			return new SingleNodeService(zuliaConfig);
		}
	}

}
