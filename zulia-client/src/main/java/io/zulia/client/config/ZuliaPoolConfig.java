package io.zulia.client.config;

import io.zulia.ZuliaRESTConstants;
import io.zulia.client.pool.ConnectionListener;
import io.zulia.client.pool.Slf4jLoggingConnectionListener;

import java.util.ArrayList;
import java.util.List;

import static io.zulia.message.ZuliaBase.Node;

public class ZuliaPoolConfig {

	private int maxConcurrentRequests;
	private int defaultRetries;
	private String poolName;
	private boolean compressedConnection;
	private boolean routingEnabled;
	private boolean nodeUpdateEnabled;
	private int nodeUpdateInterval;
	private ConnectionListener connectionListener = new Slf4jLoggingConnectionListener();
	private final List<Node> nodes;
	public final static int DEFAULT_DEFAULT_RETRIES = 0;
	public final static int DEFAULT_MEMBER_UPDATE_INTERVAL = 10000;

	public static ZuliaPoolConfig localhost() {
		return new ZuliaPoolConfig().addNode("localhost");
	}

	public ZuliaPoolConfig() {
		this.nodes = new ArrayList<>();
		this.maxConcurrentRequests = 32;
		this.defaultRetries = DEFAULT_DEFAULT_RETRIES;
		this.poolName = null;
		this.compressedConnection = false;
		this.routingEnabled = true;
		this.nodeUpdateEnabled = true;
		this.nodeUpdateInterval = DEFAULT_MEMBER_UPDATE_INTERVAL;
	}

	public ZuliaPoolConfig addNode(String serverAddress) {
		return addNode(serverAddress, ZuliaRESTConstants.DEFAULT_SERVICE_SERVICE_PORT, ZuliaRESTConstants.DEFAULT_REST_SERVICE_PORT);
	}

	public ZuliaPoolConfig addNode(String serverAddress, int externalPort) {
		return addNode(serverAddress, externalPort, ZuliaRESTConstants.DEFAULT_REST_SERVICE_PORT);
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

	public int getMaxConcurrentRequests() {
		return maxConcurrentRequests;
	}

	public ZuliaPoolConfig setMaxConcurrentRequests(int maxConcurrentRequests) {
		this.maxConcurrentRequests = maxConcurrentRequests;
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

	public boolean isNodeUpdateEnabled() {
		return nodeUpdateEnabled;
	}

	public ZuliaPoolConfig setNodeUpdateEnabled(boolean nodeUpdateEnabled) {
		this.nodeUpdateEnabled = nodeUpdateEnabled;
		return this;
	}

	public int getNodeUpdateInterval() {
		return nodeUpdateInterval;
	}

	public ZuliaPoolConfig setNodeUpdateInterval(int nodeUpdateInterval) {

		if (nodeUpdateInterval < 100) {
			throw new IllegalArgumentException("Member update interval is less than the minimum of 100");
		}

		this.nodeUpdateInterval = nodeUpdateInterval;
		return this;
	}

	public ConnectionListener getConnectionListener() {
		return connectionListener;
	}

	public void setConnectionListener(ConnectionListener connectionListener) {
		this.connectionListener = connectionListener;
	}
}
