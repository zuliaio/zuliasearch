package io.zulia.server.search.aggregation.ordinal;

import org.apache.lucene.util.BytesRef;

public abstract class OrdinalBufferBase implements FacetHandler {
	protected byte[] bytes;
	protected int startPos;
	protected int pos;
	protected int limit;

	public void reset(BytesRef bytesRef) {
		this.bytes = bytesRef.bytes;
		this.startPos = bytesRef.offset;
		this.limit = bytesRef.offset + bytesRef.length;
	}

	protected int readInt() {
		int v = ((bytes[pos] & 0xFF) << 24) | ((bytes[pos + 1] & 0xFF) << 16) | ((bytes[pos + 2] & 0xFF) << 8) | (bytes[pos + 3] & 0xFF);
		pos += 4;
		return v;
	}

	protected boolean hasRemaining() {
		return pos < limit;
	}

	protected void skip(int count) {
		pos += count * 4;
	}
}
