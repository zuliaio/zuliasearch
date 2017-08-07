package io.zulia.client.config;

import io.zulia.ZuliaConstants;

import java.util.ArrayList;
import java.util.List;

import static io.zulia.message.ZuliaBase.Node;

public class ZuliaPoolConfig {

	private List<Node> nodes;

	private int maxConnections;
	private int maxIdle;
	private int defaultRetries;
	private String poolName;
	private boolean compressedConnection;
	private boolean routingEnabled;
	private boolean memberUpdateEnabled;
	private int memberUpdateInterval;

	public final static int DEFAULT_DEFAULT_RETRIES = 0;
	public final static int DEFAULT_MEMBER_UPDATE_INTERVAL = 10000;

	public ZuliaPoolConfig() {
		this.nodes = new ArrayList<>();
		this.maxConnections = 16;
		this.maxIdle = 16;
		this.defaultRetries = DEFAULT_DEFAULT_RETRIES;
		this.poolName = null;
		this.compressedConnection = false;
		this.routingEnabled = true;
		this.memberUpdateEnabled = true;
		this.memberUpdateInterval = DEFAULT_MEMBER_UPDATE_INTERVAL;
	}

	public ZuliaPoolConfig addNode(String serverAddress) {
		return addNode(serverAddress, ZuliaConstants.DEFAULT_SERVICE_SERVICE_PORT, ZuliaConstants.DEFAULT_REST_SERVICE_PORT);
	}

	public ZuliaPoolConfig addNode(String serverAddress, int externalPort) {
		return addNode(serverAddress, externalPort, ZuliaConstants.DEFAULT_REST_SERVICE_PORT);
	}

	public ZuliaPoolConfig addNode(String serverAddress, int servicePort, int restPort) {
		Node node = Node.newBuilder().setServerAddress(serverAddress).setServicePort(servicePort).setRestPort(restPort).build();
		nodes.add(node);
		return this;
	}

	public ZuliaPoolConfig addNode(Node node) {
		nodes.add(node);
		return this;
	}

	public ZuliaPoolConfig clearMembers() {
		nodes.clear();
		return this;
	}

	public List<Node> getNodes() {
		return nodes;
	}

	public int getMaxConnections() {
		return maxConnections;
	}

	public ZuliaPoolConfig setMaxConnections(int maxConnections) {
		this.maxConnections = maxConnections;
		return this;
	}

	public int getMaxIdle() {
		return maxIdle;
	}

	public ZuliaPoolConfig setMaxIdle(int maxIdle) {
		this.maxIdle = maxIdle;
		return this;
	}

	public int getDefaultRetries() {
		return defaultRetries;
	}

	public ZuliaPoolConfig setDefaultRetries(int defaultRetries) {
		this.defaultRetries = defaultRetries;
		return this;
	}

	public String getPoolName() {
		return poolName;
	}

	public ZuliaPoolConfig setPoolName(String poolName) {
		this.poolName = poolName;
		return this;
	}

	public boolean isCompressedConnection() {
		return compressedConnection;
	}

	public ZuliaPoolConfig setCompressedConnection(boolean compressedConnection) {
		this.compressedConnection = compressedConnection;
		return this;
	}

	public boolean isRoutingEnabled() {
		return routingEnabled;
	}

	public ZuliaPoolConfig setRoutingEnabled(boolean routingEnabled) {
		this.routingEnabled = routingEnabled;
		return this;
	}

	public boolean isMemberUpdateEnabled() {
		return memberUpdateEnabled;
	}

	public ZuliaPoolConfig setMemberUpdateEnabled(boolean memberUpdateEnabled) {
		this.memberUpdateEnabled = memberUpdateEnabled;
		return this;
	}

	public int getMemberUpdateInterval() {
		return memberUpdateInterval;
	}

	public ZuliaPoolConfig setMemberUpdateInterval(int memberUpdateInterval) {
		this.memberUpdateInterval = memberUpdateInterval;
		return this;
	}

}
