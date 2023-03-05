package io.zulia.server.search.stat;

public class IntLongStats extends LongStats {
	public IntLongStats(double precision) {
		super(precision);
	}

	@Override
	public void handleDocValue(long docValue) {
		newValue((int) docValue);
	}
}
