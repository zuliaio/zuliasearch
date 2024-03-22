package io.zulia.testing;

public final class IndexConfig {

	private String indexName;

	private String connection;

	public IndexConfig() {
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
		return "IndexConfig{" + "indexName='" + indexName + '\'' + ", connection='" + connection + '\'' + '}';
	}
}
