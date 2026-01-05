package io.zulia.server.search.aggregation.facets;

import io.zulia.server.search.aggregation.ordinal.FacetHandler;
import io.zulia.server.search.aggregation.ordinal.NoOpHandler;
import org.apache.lucene.search.DocIdSetIterator;

import java.io.IOException;

public class NoOpReader implements FacetsReader {

	public static final NoOpReader INSTANCE = new NoOpReader();

	private NoOpReader() {

	}

	public DocIdSetIterator getCombinedIterator(DocIdSetIterator docs) {
		return docs;
	}

	public FacetHandler getFacetHandler() {
		return NoOpHandler.INSTANCE;
	}


}