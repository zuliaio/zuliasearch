package io.zulia.client.command;

import io.zulia.client.command.base.ShardRoutableCommand;
import io.zulia.client.command.base.SimpleCommand;
import io.zulia.client.pool.ZuliaConnection;
import io.zulia.client.result.FetchResult;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

import static io.zulia.message.ZuliaQuery.FetchType;
import static io.zulia.message.ZuliaServiceGrpc.ZuliaServiceBlockingStub;
import static io.zulia.message.ZuliaServiceOuterClass.FetchRequest;
import static io.zulia.message.ZuliaServiceOuterClass.FetchResponse;

public class Fetch extends SimpleCommand<FetchRequest, FetchResult> implements ShardRoutableCommand {

	private String uniqueId;
	private String indexName;
	private String fileName;
	private FetchType resultFetchType;
	private FetchType associatedFetchType;

	private Set<String> documentFields = Collections.emptySet();
	private Set<String> documentMaskedFields = Collections.emptySet();

	public Fetch(String uniqueId, String indexName) {
		this.uniqueId = uniqueId;
		this.indexName = indexName;
	}

	public String getUniqueId() {
		return uniqueId;
	}

	@Override
	public String getIndexName() {
		return indexName;
	}

	public Fetch setFileName(String fileName) {
		this.fileName = fileName;
		return this;
	}

	public String getFileFame() {
		return fileName;
	}

	public FetchType getResultFetchType() {
		return resultFetchType;
	}

	public Fetch setResultFetchType(FetchType resultFetchType) {
		this.resultFetchType = resultFetchType;
		return this;
	}

	public FetchType getAssociatedFetchType() {
		return associatedFetchType;
	}

	public Fetch setAssociatedFetchType(FetchType associatedFetchType) {
		this.associatedFetchType = associatedFetchType;
		return this;
	}

	public Set<String> getDocumentMaskedFields() {
		return documentMaskedFields;
	}

	public Fetch addDocumentMaskedField(String documentMaskedField) {
		if (documentMaskedFields.isEmpty()) {
			documentMaskedFields = new LinkedHashSet<>();
		}

		documentMaskedFields.add(documentMaskedField);
		return this;
	}

	public Set<String> getDocumentFields() {
		return documentFields;
	}

	public Fetch addDocumentField(String documentField) {
		if (documentFields.isEmpty()) {
			this.documentFields = new LinkedHashSet<>();
		}
		documentFields.add(documentField);
		return this;
	}

	@Override
	public FetchRequest getRequest() {
		FetchRequest.Builder fetchRequestBuilder = FetchRequest.newBuilder();
		if (uniqueId != null) {
			fetchRequestBuilder.setUniqueId(uniqueId);
		}
		if (indexName != null) {
			fetchRequestBuilder.setIndexName(indexName);
		}
		if (fileName != null) {
			fetchRequestBuilder.setFilename(fileName);
		}
		if (resultFetchType != null) {
			fetchRequestBuilder.setResultFetchType(resultFetchType);
		}
		if (associatedFetchType != null) {
			fetchRequestBuilder.setAssociatedFetchType(associatedFetchType);
		}

		fetchRequestBuilder.addAllDocumentFields(documentFields);
		fetchRequestBuilder.addAllDocumentMaskedFields(documentMaskedFields);

		return fetchRequestBuilder.build();
	}

	@Override
	public FetchResult execute(ZuliaConnection zuliaConnection) {

		ZuliaServiceBlockingStub service = zuliaConnection.getService();

		FetchResponse fetchResponse = service.fetch(getRequest());

		return new FetchResult(fetchResponse);

	}

}
