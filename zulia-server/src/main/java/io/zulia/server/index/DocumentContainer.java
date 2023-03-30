package io.zulia.server.index;

import com.google.protobuf.ByteString;
import io.zulia.util.ZuliaUtil;
import org.apache.lucene.util.BytesRef;
import org.bson.Document;

public class DocumentContainer {

	private final byte[] byteArray;
	private final Document document;

	private final boolean hasDocument;

	public DocumentContainer(BytesRef binaryValue) {
		this(binaryValue != null ? binaryValue.bytes : null);
	}

	public DocumentContainer(ByteString byteString) {
		this(byteString.isEmpty() ? null : byteString.toByteArray());
	}

	public DocumentContainer(byte[] bytes) {
		if (bytes != null && bytes.length > 0) {
			this.byteArray = bytes;
			this.document = ZuliaUtil.byteArrayToMongoDocument(byteArray);
			this.hasDocument = true;
		}
		else {
			this.document = null;
			this.byteArray = null;
			this.hasDocument = false;
		}
	}

	public byte[] getByteArray() {
		return byteArray;
	}

	public Document getDocument() {
		return document;
	}

	public boolean hasDocument() {
		return hasDocument;
	}
}
