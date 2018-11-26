package io.zulia.client.result;

import io.zulia.util.ZuliaUtil;
import org.bson.Document;

import static io.zulia.message.ZuliaBase.AssociatedDocument;

public class AssociatedResult {

	private AssociatedDocument associatedDocument;

	public AssociatedResult(AssociatedDocument associatedDocument) {
		this.associatedDocument = associatedDocument;
	}

	public Document getMeta() {
		return ZuliaUtil.byteArrayToMongoDocument(associatedDocument.getMetadata().toByteArray());
	}

	public String getFilename() {
		return associatedDocument.getFilename();
	}

	public String getDocumentUniqueId() {
		return associatedDocument.getDocumentUniqueId();
	}

	public byte[] getDocumentAsBytes() {
		if (hasDocument()) {
			return associatedDocument.getDocument().toByteArray();
		}
		return null;
	}

	public String getDocumentAsUtf8() {
		if (hasDocument()) {
			return associatedDocument.getDocument().toStringUtf8();
		}
		return null;
	}

	public boolean hasDocument() {
		return associatedDocument.getDocument() != null;
	}

	public long getTimestamp() {
		return associatedDocument.getTimestamp();
	}

}
