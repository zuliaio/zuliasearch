package io.zulia.server.search.aggregation.stats;

public class LongLongStats extends LongStats {
	public LongLongStats(double precision) {
		super(precision);
	}

	@Override
	public void handleDocValue(long docValue) {
		newValue(docValue);
	}
}
