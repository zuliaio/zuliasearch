package io.zulia.server.config;

import io.zulia.server.config.cluster.MongoAuth;
import io.zulia.server.config.cluster.MongoServer;
import io.zulia.server.config.cluster.S3Config;
import jakarta.inject.Singleton;

import java.util.Collections;
import java.util.List;

@Singleton
public class ZuliaConfig {

	private String dataPath = "data";
	private boolean cluster = false;
	private String clusterName = "zulia";
	private String clusterStorageEngine = "gridfs";
	private S3Config s3;
	private List<MongoServer> mongoServers = Collections.singletonList(new MongoServer());
	private MongoAuth mongoAuth;
	private String serverAddress = null; //null means autodetect
	private int servicePort = 32191;
	private int restPort = 32192;

	public ZuliaConfig() {
	}

	public String getDataPath() {
		return dataPath;
	}

	public void setDataPath(String dataPath) {
		this.dataPath = dataPath;
	}

	public boolean isCluster() {
		return cluster;
	}

	public void setCluster(boolean cluster) {
		this.cluster = cluster;
	}

	public String getClusterName() {
		return clusterName;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	public String getClusterStorageEngine() {
		return clusterStorageEngine;
	}

	public void setClusterStorageEngine(String clusterStorageEngine) {
		this.clusterStorageEngine = clusterStorageEngine;
	}

	public S3Config getS3() {
		return s3;
	}

	public void setS3(S3Config s3) {
		this.s3 = s3;
	}

	public List<MongoServer> getMongoServers() {
		return mongoServers;
	}

	public void setMongoServers(List<MongoServer> mongoServers) {
		this.mongoServers = mongoServers;
	}

	public MongoAuth getMongoAuth() {
		return mongoAuth;
	}

	public void setMongoAuth(MongoAuth mongoAuth) {
		this.mongoAuth = mongoAuth;
	}

	public String getServerAddress() {
		return serverAddress;
	}

	public void setServerAddress(String serverAddress) {
		this.serverAddress = serverAddress;
	}

	public int getServicePort() {
		return servicePort;
	}

	public void setServicePort(int servicePort) {
		this.servicePort = servicePort;
	}

	public int getRestPort() {
		return restPort;
	}

	public void setRestPort(int restPort) {
		this.restPort = restPort;
	}

	@Override
	public String toString() {
		return "ZuliaConfig{" + "dataPath='" + dataPath + '\'' + ", cluster=" + cluster + ", clusterName='" + clusterName + '\'' + ", clusterStorageEngine='"
				+ clusterStorageEngine + '\'' + ", s3=" + s3 + ", mongoServers=" + mongoServers + ", mongoAuth=" + mongoAuth + ", serverAddress='"
				+ serverAddress + '\'' + ", servicePort=" + servicePort + ", restPort=" + restPort + '}';
	}
}
