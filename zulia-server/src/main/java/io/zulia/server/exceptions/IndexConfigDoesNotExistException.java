package io.zulia.server.exceptions;

import java.io.Serial;

public class IndexConfigDoesNotExistException extends NotFoundException {

	private static final long serialVersionUID = 1L;
	private final String indexName;

	public IndexConfigDoesNotExistException(String name) {
		super("Index config " + name + " does not exist");
		this.indexName = name;
	}

	public String getIndexName() {
		return indexName;
	}

}
