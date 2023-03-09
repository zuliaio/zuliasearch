package io.zulia.server.search.aggregation.ordinal;

import org.apache.lucene.util.BytesRef;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.function.IntConsumer;

public class OrdinalBuffer {
	private final IntBuffer ordinalBuffer;

	public OrdinalBuffer(BytesRef bytesRef) {
		ordinalBuffer = ByteBuffer.wrap(bytesRef.bytes, bytesRef.offset, bytesRef.length).asIntBuffer();
	}

	public void handleFacets(int[] requestDimensionOrdinals, IntConsumer ordinalHandler) {
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
						break;
					}

					storedDimOrdinal = ordinalBuffer.get();
					storedOrdinalLengthForDim = ordinalBuffer.get();
				}

				if (requestedDimOrdinal == storedDimOrdinal) {
					for (int i = 0; i < storedOrdinalLengthForDim; i++) {
						ordinalHandler.accept(ordinalBuffer.get());
					}
					storedOrdinalLengthForDim = 0;
				}

			}
		}
	}

}
