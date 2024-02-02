package io.zulia.server.exceptions;

public class AssociatedDocumentDoesNotExistException extends NotFoundException {

	private static final long serialVersionUID = 1L;
	private String uniqueId;
	private String indexName;

	public AssociatedDocumentDoesNotExistException(String indexName, String uniqueId, String fileName) {
		super("Associated Document <" + uniqueId + "> does not exist for index <" + indexName + "> with file name <" + fileName + ">");
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
