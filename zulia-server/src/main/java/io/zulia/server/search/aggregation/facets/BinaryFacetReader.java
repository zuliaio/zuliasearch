package io.zulia.server.search.aggregation.facets;

import io.zulia.server.search.aggregation.ordinal.FacetHandler;
import io.zulia.server.search.aggregation.ordinal.MultiDimensionOrdinalBuffer;
import io.zulia.server.search.aggregation.ordinal.NoOpHandler;
import io.zulia.server.search.aggregation.ordinal.OrdinalBufferBase;
import io.zulia.server.search.aggregation.ordinal.SingleDimensionOrdinalBuffer;

import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.DocIdSetIterator;

import java.io.IOException;
import java.util.Arrays;

public class BinaryFacetReader implements FacetsReader {
	private final BinaryDocValues ordinalBinaryValues;
	private final FacetHandler reusableHandler;

	public BinaryFacetReader(LeafReader reader, String facetField, boolean individualFacet) throws IOException {
		this.ordinalBinaryValues = reader.getBinaryDocValues(facetField);
		this.reusableHandler = ordinalBinaryValues == null ? NoOpHandler.INSTANCE :
				individualFacet ? new SingleDimensionOrdinalBuffer() : new MultiDimensionOrdinalBuffer();
	}

	@Override
	public DocIdSetIterator getCombinedIterator(DocIdSetIterator docs) {
		if (ordinalBinaryValues == null) {
			return docs;
		}
		return ConjunctionUtils.intersectIterators(Arrays.asList(docs, ordinalBinaryValues));
	}

	@Override
	public FacetHandler getFacetHandler() throws IOException {
		if (reusableHandler instanceof OrdinalBufferBase buffer) {
			buffer.reset(ordinalBinaryValues.binaryValue());
		}
		return reusableHandler;
	}

}
