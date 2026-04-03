package io.zulia.server.search.aggregation.ordinal;

public class SingleDimensionOrdinalBuffer extends OrdinalBufferBase {

	public void handleFacets(OrdinalConsumer ordinalConsumer) {
		pos = startPos;
		int storedOrdinalLengthForDim = readInt();
		for (int i = 0; i < storedOrdinalLengthForDim; i++) {
			ordinalConsumer.handleOrdinal(readInt());
		}
	}

}
