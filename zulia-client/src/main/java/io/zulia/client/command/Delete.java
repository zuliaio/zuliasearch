package io.zulia.client.command;

import io.zulia.client.command.base.ShardRoutableCommand;
import io.zulia.client.command.base.SimpleCommand;
import io.zulia.client.pool.ZuliaConnection;
import io.zulia.client.result.DeleteResult;
import io.zulia.message.ZuliaServiceOuterClass.DeleteRequest;

import static io.zulia.message.ZuliaServiceGrpc.ZuliaServiceBlockingStub;
import static io.zulia.message.ZuliaServiceOuterClass.DeleteResponse;

public abstract class Delete extends SimpleCommand<DeleteRequest, DeleteResult> implements ShardRoutableCommand {
	private String indexName;
	private String uniqueId;
	private String fileName;
	private Boolean deleteDocument;
	private Boolean deleteAllAssociated;

	public Delete(String uniqueId, String indexName) {
		this.uniqueId = uniqueId;
		this.indexName = indexName;
	}

	@Override
	public String getUniqueId() {
		return uniqueId;
	}

	protected void setIndexName(String indexName) {
		this.indexName = indexName;
	}

	@Override
	public String getIndexName() {
		return indexName;
	}

	protected Delete setFileName(String fileName) {
		this.fileName = fileName;
		return this;
	}

	protected String getFileName() {
		return fileName;
	}

	protected Delete setDeleteDocument(Boolean deleteDocument) {
		this.deleteDocument = deleteDocument;
		return this;
	}

	protected Boolean getDeleteDocument() {
		return deleteDocument;
	}

	protected Delete setDeleteAllAssociated(Boolean deleteAllAssociated) {
		this.deleteAllAssociated = deleteAllAssociated;
		return this;
	}

	protected Boolean getDeleteAllAssociated() {
		return deleteAllAssociated;
	}

	@Override
	public DeleteRequest getRequest() {
		DeleteRequest.Builder deleteRequestBuilder = DeleteRequest.newBuilder();
		if (uniqueId != null) {
			deleteRequestBuilder.setUniqueId(uniqueId);
		}
		if (indexName != null) {
			deleteRequestBuilder.setIndexName(indexName);
		}
		if (fileName != null) {
			deleteRequestBuilder.setFilename(fileName);
		}
		if (deleteDocument != null) {
			deleteRequestBuilder.setDeleteDocument(deleteDocument);
		}
		if (deleteAllAssociated != null) {
			deleteRequestBuilder.setDeleteAllAssociated(deleteAllAssociated);
		}
		return deleteRequestBuilder.build();
	}

	@Override
	public DeleteResult execute(ZuliaConnection zuliaConnection) {
		ZuliaServiceBlockingStub service = zuliaConnection.getService();

		DeleteResponse deleteResponse = service.delete(getRequest());

		return new DeleteResult(deleteResponse);
	}

}
