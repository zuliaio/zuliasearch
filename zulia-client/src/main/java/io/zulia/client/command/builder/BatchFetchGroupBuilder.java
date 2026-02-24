package io.zulia.client.command.builder;

import io.zulia.message.ZuliaQuery.FetchType;
import io.zulia.message.ZuliaServiceOuterClass.BatchFetchGroup;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

public class BatchFetchGroupBuilder {

	private final String indexName;
	private final Set<String> uniqueIds;
	private FetchType resultFetchType = FetchType.FULL;
	private FetchType associatedFetchType = FetchType.NONE;
	private String filename = "";
	private Set<String> documentFields = Collections.emptySet();
	private Set<String> documentMaskedFields = Collections.emptySet();
	private boolean realtime;

	public BatchFetchGroupBuilder(String indexName, Collection<String> uniqueIds) {
		this.indexName = indexName;
		this.uniqueIds = new LinkedHashSet<>(uniqueIds);
	}

	public BatchFetchGroupBuilder setResultFetchType(FetchType resultFetchType) {
		this.resultFetchType = resultFetchType;
		return this;
	}

	public BatchFetchGroupBuilder setAssociatedFetchType(FetchType associatedFetchType) {
		this.associatedFetchType = associatedFetchType;
		return this;
	}

	public BatchFetchGroupBuilder setFilename(String filename) {
		this.filename = filename;
		return this;
	}

	public BatchFetchGroupBuilder addDocumentField(String documentField) {
		if (documentFields.isEmpty()) {
			documentFields = new LinkedHashSet<>();
		}
		documentFields.add(documentField);
		return this;
	}

	public BatchFetchGroupBuilder addDocumentFields(Collection<String> documentFields) {
		if (this.documentFields.isEmpty()) {
			this.documentFields = new LinkedHashSet<>();
		}
		this.documentFields.addAll(documentFields);
		return this;
	}

	public BatchFetchGroupBuilder addDocumentMaskedField(String documentMaskedField) {
		if (documentMaskedFields.isEmpty()) {
			documentMaskedFields = new LinkedHashSet<>();
		}
		documentMaskedFields.add(documentMaskedField);
		return this;
	}

	public BatchFetchGroupBuilder addDocumentMaskedFields(Collection<String> documentMaskedFields) {
		if (this.documentMaskedFields.isEmpty()) {
			this.documentMaskedFields = new LinkedHashSet<>();
		}
		this.documentMaskedFields.addAll(documentMaskedFields);
		return this;
	}

	public BatchFetchGroupBuilder setRealtime(boolean realtime) {
		this.realtime = realtime;
		return this;
	}

	public BatchFetchGroup build() {
		BatchFetchGroup.Builder builder = BatchFetchGroup.newBuilder();
		builder.setIndexName(indexName);
		builder.addAllUniqueId(uniqueIds);
		builder.setResultFetchType(resultFetchType);
		builder.setAssociatedFetchType(associatedFetchType);
		builder.setFilename(filename);
		builder.addAllDocumentFields(documentFields);
		builder.addAllDocumentMaskedFields(documentMaskedFields);
		builder.setRealtime(realtime);
		return builder.build();
	}
}
