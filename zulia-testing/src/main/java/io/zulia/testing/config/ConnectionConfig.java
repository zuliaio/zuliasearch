package io.zulia.testing.config;

public class ConnectionConfig {

	private String name;

	private String serverAddress;

	private int port;

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

	@Override
	public String toString() {
		return "ConnectionConfig{" + "name='" + name + '\'' + ", serverAddress='" + serverAddress + '\'' + ", port=" + port + '}';
	}
}


