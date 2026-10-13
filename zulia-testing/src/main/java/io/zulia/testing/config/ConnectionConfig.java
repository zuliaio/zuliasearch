package io.zulia.testing.config;

public class ConnectionConfig {

	private String name;

	private String serverAddress;

	private int port = 32191;

	private boolean routingEnabled = false;

	public ConnectionConfig() {

	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getServerAddress() {
		return serverAddress;
	}

	public void setServerAddress(String serverAddress) {
		this.serverAddress = serverAddress;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	public boolean isRoutingEnabled() {
		return routingEnabled;
	}

	/**
	 * Enables node discovery and routing of each request to the node that owns the shard.
	 * Leave off when the address is a port forward or a load balancer, otherwise requests go to the cluster's own addresses.
	 */
	public void setRoutingEnabled(boolean routingEnabled) {
		this.routingEnabled = routingEnabled;
	}

	@Override
	public String toString() {
		return "ConnectionConfig{" + "name='" + name + '\'' + ", serverAddress='" + serverAddress + '\'' + ", port=" + port + ", routingEnabled=" + routingEnabled + '}';
	}
}


