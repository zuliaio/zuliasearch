package io.zulia.server.search.aggregation.ordinal;

import java.io.IOException;

public interface FacetHandler {
	void handleFacets(OrdinalConsumer ordinalConsumer) throws IOException;
}
