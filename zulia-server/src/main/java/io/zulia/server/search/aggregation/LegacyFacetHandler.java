package io.zulia.server.search.aggregation;

import io.zulia.server.search.aggregation.ordinal.FacetHandler;
import io.zulia.server.search.aggregation.ordinal.OrdinalConsumer;
import org.apache.lucene.index.SortedNumericDocValues;

import java.io.IOException;

public class LegacyFacetHandler implements FacetHandler {
	private final int[] ordinalDocValues;

	public LegacyFacetHandler(SortedNumericDocValues legacyOrdinalValues) throws IOException {

		if (legacyOrdinalValues != null) {

			int ordinalCount = legacyOrdinalValues.docValueCount();

			this.ordinalDocValues = new int[ordinalCount];

			for (int i = 0; i < ordinalCount; i++) {
				int ordIndex = (int) legacyOrdinalValues.nextValue();
				this.ordinalDocValues[i] = ordIndex;
			}
		}
		else {
			this.ordinalDocValues = new int[0];
		}

	}

	@Override
	public void handleFacets(OrdinalConsumer ordinalConsumer) {
		for (int ordinalDocValue : ordinalDocValues) {
			ordinalConsumer.handleOrdinal(ordinalDocValue);
		}
	}
}
