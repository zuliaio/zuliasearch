package io.zulia.testing.config;

public final class IndexConfig {

	private String name;

	private String indexName;

	private String connection;

	public IndexConfig() {
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getIndexName() {
		return indexName;
	}

	public void setIndexName(String indexName) {
		this.indexName = indexName;
	}

	public String getConnection() {
		return connection;
	}

	public void setConnection(String connection) {
		this.connection = connection;
	}

	@Override
	public String toString() {
		return "IndexConfig{" + "name='" + name + '\'' + ", indexName='" + indexName + '\'' + ", connection='" + connection + '\'' + '}';
	}
}
