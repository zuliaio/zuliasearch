package io.zulia.testing;

public class ConnectionConfig {

	private String serverAddress;

	private int port;

	public ConnectionConfig() {

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
		return "ConnectionConfig{" + "serverAddress='" + serverAddress + '\'' + ", port=" + port + '}';
	}
}


