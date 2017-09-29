package io.zulia.server.exceptions;

import java.io.IOException;

public class ShardDoesNotExistException extends IOException {

	private static final long serialVersionUID = 1L;
	private String indexName;
	private int shardNumber;

	public ShardDoesNotExistException(String indexName, int shardNumber) {
		super("Shard does not exist for index <" + indexName + "> with shard <" + shardNumber + ">");
		this.indexName = indexName;
		this.shardNumber = shardNumber;
	}

	public String getIndexName() {
		return indexName;
	}

	public int getShardNumber() {
		return shardNumber;
	}
}
