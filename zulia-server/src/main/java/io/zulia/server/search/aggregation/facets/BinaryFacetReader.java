package io.zulia.server.search.aggregation.facets;

import io.zulia.server.search.aggregation.ordinal.FacetHandler;
import io.zulia.server.search.aggregation.ordinal.MultiDimensionOrdinalBuffer;
import io.zulia.server.search.aggregation.ordinal.SingleDimensionOrdinalBuffer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.DocIdSetIterator;

import java.io.IOException;
import java.util.Arrays;

public class BinaryFacetReader implements FacetsReader {
	private final BinaryDocValues ordinalBinaryValues;
	private final boolean individualFacet;

	public BinaryFacetReader(LeafReader reader, String facetField, boolean individualFacet) throws IOException {
		ordinalBinaryValues = reader.getBinaryDocValues(facetField);
		this.individualFacet = individualFacet;
	}

	@Override
	public DocIdSetIterator getCombinedIterator(DocIdSetIterator docs) {
		return ConjunctionUtils.intersectIterators(Arrays.asList(docs, ordinalBinaryValues));
	}

	@Override
	public FacetHandler getFacetHandler() throws IOException {
		if (individualFacet) {
			return new SingleDimensionOrdinalBuffer(ordinalBinaryValues.binaryValue());
		}
		else {
			return new MultiDimensionOrdinalBuffer(ordinalBinaryValues.binaryValue());
		}
	}

	@Override
	public boolean hasValues() {
		return ordinalBinaryValues != null;
	}
}
