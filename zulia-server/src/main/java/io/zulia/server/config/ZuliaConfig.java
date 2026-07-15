package io.zulia.server.config;

import io.zulia.server.config.cluster.MongoAuth;
import io.zulia.server.config.cluster.MongoConnectionString;
import io.zulia.server.config.cluster.MongoServer;
import io.zulia.server.config.cluster.S3Config;
import jakarta.inject.Singleton;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import java.util.Collections;
import java.util.List;

@Singleton
@NullMarked
public class ZuliaConfig {

	private String dataPath = "data";
	private boolean cluster = false;
	private String clusterName = "zulia";
	private String clusterStorageEngine = "gridfs";
	private @Nullable S3Config s3;
	private List<MongoServer> mongoServers = Collections.singletonList(new MongoServer());

	private @Nullable MongoConnectionString mongoConnection;
	private @Nullable MongoAuth mongoAuth;
	private @Nullable String serverAddress = null; //null means autodetect
	private int servicePort = 32191;
	private int restPort = 32192;

	private HealthConfig health = new HealthConfig();

	private boolean responseCompression;

	private int rpcWorkers;

	private int defaultConcurrency;

	private boolean debug;

	private int maxFacetsCachedPerDimension;

	private int hitsPerConcurrentRequest;

	// Minutes; deadline on the bidi segment-file stream. Raise if force-merges or vector segments produce
	// files large enough that a 10-minute ship can't finish.
	private int replicaResponseTimeout = 10;

	// Aggregate cap across all replication streams leaving this node, in bytes/sec. 0 = unlimited. Prevents
	// bootstrap of a large index from saturating the network and impacting query traffic. Default 60 MiB/s.
	private long replicationMaxBytesPerSec = 60L * 1024 * 1024;

	// Maximum number of indexes kept loaded in memory on this node. 0 = unlimited (every index stays
	// resident, the historical behavior). When exceeded, the longest-idle evictable index is unloaded to
	// disk and reloaded on demand.
	private int transientIndexCacheSize = 0;

	// Unload an index after it has gone unaccessed for this long, independent of the cache size bound.
	// 0 = disabled. Setting either this or transientIndexCacheSize enables lazy loading and eviction.
	private int transientIndexIdleTimeoutSeconds = 0;

	// Whether indexes that have replicas are eligible for eviction. Off by default: evicting a replicated
	// primary pauses its replication push until reload, and inbound pushes reload evicted replicas, so
	// write-active replicated indexes would cycle in and out of memory.
	private boolean transientIndexEvictReplicated = false;

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

	public @Nullable S3Config getS3() {
		return s3;
	}

	public void setS3(@Nullable S3Config s3) {
		this.s3 = s3;
	}

	public List<MongoServer> getMongoServers() {
		return mongoServers;
	}

	public void setMongoServers(List<MongoServer> mongoServers) {
		this.mongoServers = mongoServers;
	}

	public @Nullable MongoConnectionString getMongoConnection() {
		return mongoConnection;
	}

	public void setMongoConnection(@Nullable MongoConnectionString mongoConnection) {
		this.mongoConnection = mongoConnection;
	}

	public @Nullable MongoAuth getMongoAuth() {
		return mongoAuth;
	}

	public void setMongoAuth(@Nullable MongoAuth mongoAuth) {
		this.mongoAuth = mongoAuth;
	}

	public @Nullable String getServerAddress() {
		return serverAddress;
	}

	public void setServerAddress(@Nullable String serverAddress) {
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

	public boolean isResponseCompression() {
		return responseCompression;
	}

	public void setResponseCompression(boolean responseCompression) {
		this.responseCompression = responseCompression;
	}

	public int getRpcWorkers() {
		return rpcWorkers;
	}

	public void setRpcWorkers(int rpcWorkers) {
		this.rpcWorkers = rpcWorkers;
	}

	public int getDefaultConcurrency() {
		return defaultConcurrency;
	}

	public void setDefaultConcurrency(int defaultConcurrency) {
		this.defaultConcurrency = defaultConcurrency;
	}

	public boolean isDebug() {
		return debug;
	}

	public void setDebug(boolean debug) {
		this.debug = debug;
	}

	public int getMaxFacetsCachedPerDimension() {
		return maxFacetsCachedPerDimension;
	}

	public void setMaxFacetsCachedPerDimension(int maxFacetsCachedPerDimension) {
		this.maxFacetsCachedPerDimension = maxFacetsCachedPerDimension;
	}

	public int getHitsPerConcurrentRequest() {
		return hitsPerConcurrentRequest;
	}

	public void setHitsPerConcurrentRequest(int hitsPerConcurrentRequest) {
		this.hitsPerConcurrentRequest = hitsPerConcurrentRequest;
	}

	public HealthConfig getHealth() {
		return health;
	}

	public void setHealth(HealthConfig health) {
		this.health = health;
	}

	public int getReplicaResponseTimeout() {
		return replicaResponseTimeout;
	}

	public void setReplicaResponseTimeout(int replicaResponseTimeout) {
		this.replicaResponseTimeout = replicaResponseTimeout;
	}

	public long getReplicationMaxBytesPerSec() {
		return replicationMaxBytesPerSec;
	}

	public void setReplicationMaxBytesPerSec(long replicationMaxBytesPerSec) {
		this.replicationMaxBytesPerSec = replicationMaxBytesPerSec;
	}

	public int getTransientIndexCacheSize() {
		return transientIndexCacheSize;
	}

	public void setTransientIndexCacheSize(int transientIndexCacheSize) {
		this.transientIndexCacheSize = transientIndexCacheSize;
	}

	public int getTransientIndexIdleTimeoutSeconds() {
		return transientIndexIdleTimeoutSeconds;
	}

	public void setTransientIndexIdleTimeoutSeconds(int transientIndexIdleTimeoutSeconds) {
		this.transientIndexIdleTimeoutSeconds = transientIndexIdleTimeoutSeconds;
	}

	public boolean isTransientIndexEvictReplicated() {
		return transientIndexEvictReplicated;
	}

	public void setTransientIndexEvictReplicated(boolean transientIndexEvictReplicated) {
		this.transientIndexEvictReplicated = transientIndexEvictReplicated;
	}

	@Override
	public String toString() {
		return "ZuliaConfig{" + "dataPath='" + dataPath + '\'' + ", cluster=" + cluster + ", clusterName='" + clusterName + '\'' + ", clusterStorageEngine='"
				+ clusterStorageEngine + '\'' + ", s3=" + s3 + ", mongoServers=" + mongoServers + ", mongoConnection=" + mongoConnection + ", mongoAuth="
				+ mongoAuth + ", serverAddress='" + serverAddress + '\'' + ", servicePort=" + servicePort + ", restPort=" + restPort + ", health=" + health
				+ ", responseCompression=" + responseCompression + ", rpcWorkers=" + rpcWorkers + ", defaultConcurrency=" + defaultConcurrency + ", debug="
				+ debug + ", maxFacetsCachedPerDimension=" + maxFacetsCachedPerDimension + ", hitsPerConcurrentRequest=" + hitsPerConcurrentRequest
				+ ", replicaResponseTimeout=" + replicaResponseTimeout + ", replicationMaxBytesPerSec=" + replicationMaxBytesPerSec
				+ ", transientIndexCacheSize=" + transientIndexCacheSize + ", transientIndexIdleTimeoutSeconds=" + transientIndexIdleTimeoutSeconds
				+ ", transientIndexEvictReplicated=" + transientIndexEvictReplicated + '}';
	}
}
