package io.zulia.server.config.cluster;

public class MongoAuth {

	private String username;
	private String database;
	private String password;

	public MongoAuth() {
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getDatabase() {
		return database;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	@Override
	public String toString() {
		return "MongoAuth{" + "username='" + username + '\'' + ", database='" + database + '\'' + '}';
	}
}
