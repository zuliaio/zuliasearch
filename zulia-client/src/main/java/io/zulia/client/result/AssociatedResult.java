package io.zulia.client.result;

import java.util.HashMap;
import java.util.Map;

import static io.zulia.message.ZuliaBase.AssociatedDocument;
import static io.zulia.message.ZuliaBase.Metadata;

public class AssociatedResult {

	private AssociatedDocument associatedDocument;

	public AssociatedResult(AssociatedDocument associatedDocument) {
		this.associatedDocument = associatedDocument;
	}

	public Map<String, String> getMeta() {
		HashMap<String, String> metadata = new HashMap<String, String>();
		for (Metadata md : associatedDocument.getMetadataList()) {
			metadata.put(md.getKey(), md.getValue());
		}
		return metadata;
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
			String contents = associatedDocument.getDocument().toStringUtf8();
			return contents;
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
