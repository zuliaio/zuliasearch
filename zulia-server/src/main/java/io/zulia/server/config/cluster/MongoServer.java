package io.zulia.server.config.cluster;

public class MongoServer {

	private String hostname = "localhost";
	private int port = 27017;

	public MongoServer() {
	}

	public String getHostname() {
		return hostname;
	}

	public int getPort() {
		return port;
	}
}
