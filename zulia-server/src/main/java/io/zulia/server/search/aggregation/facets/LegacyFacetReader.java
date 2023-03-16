package io.zulia.server.search.aggregation.facets;

import io.zulia.server.search.aggregation.LegacyFacetHandler;
import io.zulia.server.search.aggregation.ordinal.FacetHandler;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.DocIdSetIterator;

import java.io.IOException;
import java.util.Arrays;

public class LegacyFacetReader implements FacetsReader {
	private final SortedNumericDocValues legacyOrdinalValues;

	public LegacyFacetReader(LeafReader reader) throws IOException {
		this.legacyOrdinalValues = reader.getSortedNumericDocValues("$facets");
	}

	@Override
	public DocIdSetIterator getCombinedIterator(DocIdSetIterator docs) {
		return ConjunctionUtils.intersectIterators(Arrays.asList(docs, legacyOrdinalValues));
	}

	@Override
	public FacetHandler getFacetHandler() throws IOException {
		return new LegacyFacetHandler(legacyOrdinalValues);
	}

	@Override
	public boolean hasValues() {
		return legacyOrdinalValues != null;
	}
}
