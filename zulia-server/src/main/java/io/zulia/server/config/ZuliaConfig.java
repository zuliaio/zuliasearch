package io.zulia.server.config;

import java.util.Collections;
import java.util.List;

public class ZuliaConfig {

	private String dataPath = "data";
	private boolean cluster = false;
	private String clusterName = "zulia";
	private List<MongoServer> mongoServers = Collections.singletonList(new MongoServer());
	private String serverAddress = null; //null means autodetect
	private int hazelcastPort = 5701;
	private int servicePort = 32191;
	private int restPort = 32192;

	public ZuliaConfig() {
	}

	public String getDataPath() {
		return dataPath;
	}

	public boolean isCluster() {
		return cluster;
	}

	public String getClusterName() {
		return clusterName;
	}

	public List<MongoServer> getMongoServers() {
		return mongoServers;
	}

	public String getServerAddress() {
		return serverAddress;
	}

	public void setServerAddress(String serverAddress) {
		this.serverAddress = serverAddress;
	}

	public int getHazelcastPort() {
		return hazelcastPort;
	}

	public int getServicePort() {
		return servicePort;
	}

	public int getRestPort() {
		return restPort;
	}
}
