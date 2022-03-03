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

	public int getServicePort() {
		return servicePort;
	}

	public int getRestPort() {
		return restPort;
	}

	public void setDataPath(String dataPath) {
		this.dataPath = dataPath;
	}

	public void setCluster(boolean cluster) {
		this.cluster = cluster;
	}

	public void setClusterName(String clusterName) {
		this.clusterName = clusterName;
	}

	public void setMongoServers(List<MongoServer> mongoServers) {
		this.mongoServers = mongoServers;
	}

	public void setServerAddress(String serverAddress) {
		this.serverAddress = serverAddress;
	}

	public void setServicePort(int servicePort) {
		this.servicePort = servicePort;
	}

	public void setRestPort(int restPort) {
		this.restPort = restPort;
	}

	public MongoAuth getMongoAuth() {
		return mongoAuth;
	}

	public void setMongoAuth(MongoAuth mongoAuth) {
		this.mongoAuth = mongoAuth;
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
}
