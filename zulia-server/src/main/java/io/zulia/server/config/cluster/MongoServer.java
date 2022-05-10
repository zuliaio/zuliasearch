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

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public int getPort() {
		return port;
	}

	public void setPort(int port) {
		this.port = port;
	}

	@Override
	public String toString() {
		return "{" + "hostname='" + hostname + '\'' + ", port=" + port + '}';
	}
}
