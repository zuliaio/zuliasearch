package io.zulia.doc;

import com.google.protobuf.ByteString;
import io.zulia.message.ZuliaBase.Metadata;
import io.zulia.message.ZuliaBase.ResultDocument;
import io.zulia.util.ZuliaUtil;
import org.bson.Document;

import java.util.HashMap;

public class ResultDocBuilder {
	private ResultDocument.Builder resultDocumentBuilder;

	public static ResultDocBuilder newBuilder() {
		return new ResultDocBuilder();
	}

	public ResultDocBuilder() {
		resultDocumentBuilder = ResultDocument.newBuilder();
	}

	public ResultDocBuilder addMetaData(String key, String value) {
		resultDocumentBuilder.addMetadata(Metadata.newBuilder().setKey(key).setValue(value));
		return this;
	}

	public ResultDocBuilder addAllMetaData(HashMap<String, String> metadata) {
		for (String key : metadata.keySet()) {
			resultDocumentBuilder.addMetadata(Metadata.newBuilder().setKey(key).setValue(metadata.get(key)));
		}
		return this;
	}

	public ResultDocBuilder setDocument(Document document) {
		resultDocumentBuilder.setDocument(ByteString.copyFrom(ZuliaUtil.mongoDocumentToByteArray(document)));
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
