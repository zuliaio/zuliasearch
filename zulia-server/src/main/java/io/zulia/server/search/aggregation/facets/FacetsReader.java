package io.zulia.server.search.aggregation.facets;

import io.zulia.server.search.aggregation.ordinal.FacetHandler;
import org.apache.lucene.search.DocIdSetIterator;

import java.io.IOException;

public interface FacetsReader {

	DocIdSetIterator getCombinedIterator(DocIdSetIterator docs);

	FacetHandler getFacetHandler() throws IOException;

}