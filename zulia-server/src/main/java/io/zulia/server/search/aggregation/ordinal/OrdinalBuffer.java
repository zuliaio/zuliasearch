package io.zulia.server.search.aggregation.ordinal;

import org.apache.lucene.util.BytesRef;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;

public class OrdinalBuffer implements FacetHandler {
	private final IntBuffer ordinalBuffer;

	public OrdinalBuffer(BytesRef bytesRef) {
		ordinalBuffer = ByteBuffer.wrap(bytesRef.bytes, bytesRef.offset, bytesRef.length).asIntBuffer();
	}

	public void handleFacets(OrdinalConsumer ordinalConsumer) {
		int[] requestDimensionOrdinals = ordinalConsumer.requestedDimensionOrdinals();
		ordinalBuffer.position(0);
		if (ordinalBuffer.hasRemaining()) {
			int storedDimOrdinal = -1;
			int storedOrdinalLengthForDim = 0;

			for (int requestedDimOrdinal : requestDimensionOrdinals) {

				while (storedDimOrdinal < requestedDimOrdinal) {
					if (storedOrdinalLengthForDim != 0) {
						ordinalBuffer.position(ordinalBuffer.position() + storedOrdinalLengthForDim);
					}

					if (!ordinalBuffer.hasRemaining()) {
						return;
					}

					storedDimOrdinal = ordinalBuffer.get();
					storedOrdinalLengthForDim = ordinalBuffer.get();
				}

				if (requestedDimOrdinal == storedDimOrdinal) {
					for (int i = 0; i < storedOrdinalLengthForDim; i++) {
						ordinalConsumer.handleOrdinal(ordinalBuffer.get());
					}
					storedOrdinalLengthForDim = 0;
				}

			}
		}
	}

}
