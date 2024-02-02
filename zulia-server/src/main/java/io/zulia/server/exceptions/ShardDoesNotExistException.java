package io.zulia.server.exceptions;

public class ShardDoesNotExistException extends NotFoundException {

	private static final long serialVersionUID = 1L;
	private final String indexName;
	private final int shardNumber;

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
