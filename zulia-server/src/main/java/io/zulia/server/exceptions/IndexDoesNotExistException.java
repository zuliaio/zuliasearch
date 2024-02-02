package io.zulia.server.exceptions;

public class IndexDoesNotExistException extends NotFoundException {

	private static final long serialVersionUID = 1L;
	private String indexName;

	public IndexDoesNotExistException(String indexName) {
		super("Index <" + indexName + "> does not exist");
		this.indexName = indexName;
	}

	public String getIndexName() {
		return indexName;
	}

}
