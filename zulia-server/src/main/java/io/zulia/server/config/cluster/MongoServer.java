package io.zulia.server.config.cluster;

public class MongoServer {

	private String hostname = "localhost";
	private int port = 27017;

	public MongoServer() {
	}

	public MongoServer(String hostname, int port) {
		this.hostname = hostname;
		this.port = port;
	}

	public String getHostname() {
		return hostname;
	}

	public int getPort() {
		return port;
	}
}
