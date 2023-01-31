package io.zulia.server.exceptions;

import java.io.IOException;

public class IndexDoesNotExistException extends IOException {

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
