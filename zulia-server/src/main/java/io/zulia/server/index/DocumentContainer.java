package io.zulia.server.index;

import com.google.protobuf.ByteString;
import io.zulia.util.ZuliaUtil;
import org.bson.Document;

public class DocumentContainer {

	private final byte[] byteArray;
	private final Document document;

	private final boolean isEmpty;

	public DocumentContainer(ByteString byteString) {
		this(byteString.isEmpty() ? null : byteString.toByteArray());
	}

	public DocumentContainer(byte[] bytes) {
		if (bytes != null && bytes.length > 0) {
			this.byteArray = bytes;
			this.document = ZuliaUtil.byteArrayToMongoDocument(byteArray);
			this.isEmpty = false;
		}
		else {
			this.document = null;
			this.byteArray = null;
			this.isEmpty = true;
		}
	}

	public byte[] getByteArray() {
		return byteArray;
	}

	public Document getDocument() {
		return document;
	}

	public boolean isEmpty() {
		return isEmpty;
	}
}
