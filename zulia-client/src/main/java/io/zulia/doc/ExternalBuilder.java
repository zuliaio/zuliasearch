package io.zulia.doc;

import io.zulia.message.ZuliaBase;
import io.zulia.util.ZuliaUtil;
import org.bson.Document;

public class ExternalBuilder {
	private ZuliaBase.ExternalDocument.Builder builder;
	private Document metadata;
	private Document locationData;

	public static ExternalBuilder newBuilder() {
		return new ExternalBuilder();
	}

	private ExternalBuilder() {
		this.builder = ZuliaBase.ExternalDocument.newBuilder();
		metadata = new Document();
		locationData = new Document();
	}

	public ExternalBuilder setFilename(String filename) {
		builder.setFilename(filename);
		return this;
	}

	public ExternalBuilder setDocumentUniqueId(String documentUniqueId) {
		builder.setDocumentUniqueId(documentUniqueId);
		return this;
	}

	public ExternalBuilder setMetadata(Document metadata) {
		this.metadata = metadata;
		return this;
	}

	public ExternalBuilder setLocationData(Document location) {
		this.locationData = location;
		return this;
	}

	public ExternalBuilder setIndexName(String indexName) {
		builder.setIndexName(indexName);
		return this;
	}

	public ZuliaBase.ExternalDocument build() {
		Document registration = new Document();

		registration.put("metdata", metadata);
		registration.put("location", locationData);

		builder.setRegistration(ZuliaUtil.mongoDocumentToByteString(registration));

		return builder.build();
	}
}
