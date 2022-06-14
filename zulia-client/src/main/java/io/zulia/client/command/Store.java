package io.zulia.client.command;

import io.zulia.client.command.base.ShardRoutableCommand;
import io.zulia.client.command.base.SimpleCommand;
import io.zulia.client.pool.ZuliaConnection;
import io.zulia.client.result.StoreResult;
import io.zulia.doc.AssociatedBuilder;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.message.ZuliaBase;
import io.zulia.message.ZuliaBase.ResultDocument;
import io.zulia.message.ZuliaServiceOuterClass.StoreRequest;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

import static io.zulia.message.ZuliaBase.AssociatedDocument;
import static io.zulia.message.ZuliaServiceGrpc.ZuliaServiceBlockingStub;
import static io.zulia.message.ZuliaServiceOuterClass.StoreResponse;

public class Store extends SimpleCommand<StoreRequest, StoreResult> implements ShardRoutableCommand {
	private String uniqueId;
	private String indexName;
	private ResultDocument resultDocument;

	private List<ZuliaBase.AssociatedDocument> associatedDocuments;
	private Boolean clearExistingAssociated;

	public Store(String uniqueId, String indexName) {
		this.uniqueId = uniqueId;
		this.indexName = indexName;
		this.associatedDocuments = new ArrayList<>();
	}

	public Store(String uniqueId, String indexName, ResultDocBuilder resultDocumentBuilder) {
		this(uniqueId, indexName);
		setResultDocument(resultDocumentBuilder);
	}

	@Override
	public String getUniqueId() {
		return uniqueId;
	}

	@Override
	public String getIndexName() {
		return indexName;
	}

	public Store setUniqueId(String uniqueId) {
		this.uniqueId = uniqueId;
		return this;
	}

	public ResultDocument getResultDocument() {
		return resultDocument;
	}


	public Store setResultDocument(String json) {
		return setResultDocument(ResultDocBuilder.newBuilder().setDocument(json));
	}

	public Store setResultDocument(Document document) {
		return setResultDocument(ResultDocBuilder.newBuilder().setDocument(document));
	}

	public Store setResultDocument(ResultDocBuilder resultDocumentBuilder) {
		resultDocumentBuilder.setUniqueId(uniqueId);
		resultDocumentBuilder.setIndexName(indexName);
		this.resultDocument = resultDocumentBuilder.getResultDocument();
		return this;
	}

	public Store addAssociatedDocument(AssociatedBuilder associatedBuilder) {
		associatedBuilder.setDocumentUniqueId(uniqueId);
		associatedBuilder.setIndexName(indexName);
		associatedDocuments.add(associatedBuilder.getAssociatedDocument());
		return this;
	}

	public List<ZuliaBase.AssociatedDocument> getAssociatedDocuments() {
		return associatedDocuments;
	}

	public Store setAssociatedDocuments(List<AssociatedDocument> associatedDocuments) {
		this.associatedDocuments = associatedDocuments;
		return this;
	}

	public Boolean isClearExistingAssociated() {
		return clearExistingAssociated;
	}

	public Store setClearExistingAssociated(Boolean clearExistingAssociated) {
		this.clearExistingAssociated = clearExistingAssociated;
		return this;
	}

	@Override
	public StoreRequest getRequest() {
		StoreRequest.Builder storeRequestBuilder = StoreRequest.newBuilder();
		storeRequestBuilder.setUniqueId(uniqueId);
		storeRequestBuilder.setIndexName(indexName);

		if (resultDocument != null) {
			storeRequestBuilder.setResultDocument(resultDocument);
		}
		if (associatedDocuments != null) {
			storeRequestBuilder.addAllAssociatedDocument(associatedDocuments);
		}

		if (clearExistingAssociated != null) {
			storeRequestBuilder.setClearExistingAssociated(clearExistingAssociated);
		}
		return storeRequestBuilder.build();
	}

	@Override
	public StoreResult execute(ZuliaConnection zuliaConnection) {
		ZuliaServiceBlockingStub service = zuliaConnection.getService();

		StoreResponse storeResponse = service.store(getRequest());

		return new StoreResult(storeResponse);
	}

}
