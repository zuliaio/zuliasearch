package io.zulia.doc;

import io.zulia.message.ZuliaBase.ResultDocument;
import io.zulia.util.ZuliaUtil;
import org.bson.Document;

public class ResultDocBuilder {
	private ResultDocument.Builder resultDocumentBuilder;

	public static ResultDocBuilder newBuilder() {
		return new ResultDocBuilder();
	}

	public static ResultDocBuilder from(Document document) {
		return new ResultDocBuilder().setDocument(document);
	}

	public ResultDocBuilder() {
		resultDocumentBuilder = ResultDocument.newBuilder();
	}

	public ResultDocBuilder setMetadata(Document metadata) {
		if (metadata != null) {
			resultDocumentBuilder.setMetadata(ZuliaUtil.mongoDocumentToByteString(metadata));
		}
		else {
			resultDocumentBuilder.clearMetadata();
		}
		return this;
	}

	public ResultDocBuilder setDocument(Document document) {
		resultDocumentBuilder.setDocument(ZuliaUtil.mongoDocumentToByteString(document));
		return this;
	}

	public ResultDocBuilder setDocument(String json) {
		Document document = ZuliaUtil.jsonToMongoDocument(json);
		resultDocumentBuilder.setDocument(ZuliaUtil.mongoDocumentToByteString(document));
		return this;
	}


	public String getUniqueId() {
		return resultDocumentBuilder.getUniqueId();
	}

	public ResultDocBuilder setUniqueId(String uniqueId) {
		resultDocumentBuilder.setUniqueId(uniqueId);
		return this;
	}

	public ResultDocBuilder setIndexName(String indexName) {
		resultDocumentBuilder.setIndexName(indexName);
		return this;
	}

	public ResultDocBuilder clearMetadata() {
		resultDocumentBuilder.clearMetadata();
		return this;
	}

	public ResultDocBuilder clear() {
		resultDocumentBuilder.clear();
		return this;
	}

	public ResultDocument getResultDocument() {
		return resultDocumentBuilder.build();
	}
}
