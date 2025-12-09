package io.zulia.server.search.aggregation.ordinal;

public class NoOpHandler implements FacetHandler {

	public final static NoOpHandler INSTANCE = new NoOpHandler();

	private NoOpHandler() {
	}

	public void handleFacets(OrdinalConsumer ordinalConsumer) {

	}

}
