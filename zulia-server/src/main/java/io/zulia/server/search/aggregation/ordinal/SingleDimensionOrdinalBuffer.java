package io.zulia.server.search.aggregation.ordinal;

import org.apache.lucene.util.BytesRef;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;

public class SingleDimensionOrdinalBuffer implements FacetHandler {
	private final IntBuffer ordinalBuffer;

	public SingleDimensionOrdinalBuffer(BytesRef bytesRef) {
		ordinalBuffer = ByteBuffer.wrap(bytesRef.bytes, bytesRef.offset, bytesRef.length).asIntBuffer();
	}

	public void handleFacets(OrdinalConsumer ordinalConsumer) {

		int storedOrdinalLengthForDim = ordinalBuffer.get();
		for (int i = 0; i < storedOrdinalLengthForDim; i++) {
			ordinalConsumer.handleOrdinal(ordinalBuffer.get());
		}

	}

}
