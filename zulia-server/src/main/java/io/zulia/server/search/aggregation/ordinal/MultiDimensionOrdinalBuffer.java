package io.zulia.server.search.aggregation.ordinal;

public class MultiDimensionOrdinalBuffer extends OrdinalBufferBase {

	public void handleFacets(OrdinalConsumer ordinalConsumer) {
		int[] requestDimensionOrdinals = ordinalConsumer.requestedDimensionOrdinals();
		pos = startPos;
		if (hasRemaining()) {
			int storedDimOrdinal = -1;
			int storedOrdinalLengthForDim = 0;

			for (int requestedDimOrdinal : requestDimensionOrdinals) {

				while (storedDimOrdinal < requestedDimOrdinal) {
					if (storedOrdinalLengthForDim != 0) {
						skip(storedOrdinalLengthForDim);
					}

					if (!hasRemaining()) {
						return;
					}

					storedDimOrdinal = readInt();
					storedOrdinalLengthForDim = readInt();
				}

				if (requestedDimOrdinal == storedDimOrdinal) {
					for (int i = 0; i < storedOrdinalLengthForDim; i++) {
						ordinalConsumer.handleOrdinal(readInt());
					}
					storedOrdinalLengthForDim = 0;
				}

			}
		}
	}

}
