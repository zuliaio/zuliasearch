package io.zulia.client.command.builder;

import io.zulia.message.ZuliaServiceOuterClass.BatchDeleteGroup;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Set;

public class BatchDeleteGroupBuilder {

	private final String indexName;
	private final Set<String> uniqueIds;
	private boolean deleteDocument = true;
	private boolean deleteAllAssociated = false;
	private String filename = "";

	public BatchDeleteGroupBuilder(String indexName, Collection<String> uniqueIds) {
		this.indexName = indexName;
		this.uniqueIds = new LinkedHashSet<>(uniqueIds);
	}

	public BatchDeleteGroupBuilder setDeleteDocument(boolean deleteDocument) {
		this.deleteDocument = deleteDocument;
		return this;
	}

	public BatchDeleteGroupBuilder setDeleteAllAssociated(boolean deleteAllAssociated) {
		this.deleteAllAssociated = deleteAllAssociated;
		return this;
	}

	public BatchDeleteGroupBuilder setFilename(String filename) {
		this.filename = filename;
		return this;
	}

	public BatchDeleteGroup build() {
		BatchDeleteGroup.Builder builder = BatchDeleteGroup.newBuilder();
		builder.setIndexName(indexName);
		builder.addAllUniqueId(uniqueIds);
		builder.setDeleteDocument(deleteDocument);
		builder.setDeleteAllAssociated(deleteAllAssociated);
		builder.setFilename(filename);
		return builder.build();
	}
}
