package io.zulia.server.exceptions;

public class DocumentDoesNotExistException extends NotFoundException {

	private static final long serialVersionUID = 1L;
	private String uniqueId;
	private String indexName;

	public DocumentDoesNotExistException(String uniqueId, String indexName) {
		super("Document <" + uniqueId + "> does not exist for index <" + indexName + ">");
		this.uniqueId = uniqueId;
		this.indexName = indexName;
	}

	public String getUniqueId() {
		return uniqueId;
	}

	public String getIndexName() {
		return indexName;
	}

}
