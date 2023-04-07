package io.zulia.server.search.aggregation.facets;

import io.zulia.ZuliaFieldConstants;
import io.zulia.server.search.aggregation.ordinal.FacetHandler;
import io.zulia.server.search.aggregation.ordinal.OrdinalBuffer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.DocIdSetIterator;

import java.io.IOException;
import java.util.Arrays;

public class BinaryFacetReader implements FacetsReader {
	private final BinaryDocValues ordinalBinaryValues;

	public BinaryFacetReader(LeafReader reader) throws IOException {
		ordinalBinaryValues = reader.getBinaryDocValues(ZuliaFieldConstants.FACET_STORAGE);
	}

	@Override
	public DocIdSetIterator getCombinedIterator(DocIdSetIterator docs) {
		return ConjunctionUtils.intersectIterators(Arrays.asList(docs, ordinalBinaryValues));
	}

	@Override
	public FacetHandler getFacetHandler() throws IOException {
		return new OrdinalBuffer(ordinalBinaryValues.binaryValue());
	}

	@Override
	public boolean hasValues() {
		return ordinalBinaryValues != null;
	}
}
