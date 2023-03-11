package io.zulia.server.search.aggregation.ordinal;

public interface OrdinalConsumer {

	void handleOrdinal(int ordinal);

	int[] requestedDimensionOrdinals();

}
