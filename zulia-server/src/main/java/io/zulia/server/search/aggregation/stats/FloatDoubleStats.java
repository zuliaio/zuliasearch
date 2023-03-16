package io.zulia.server.search.aggregation.stats;

import org.apache.lucene.util.NumericUtils;

public class FloatDoubleStats extends DoubleStats {
	public FloatDoubleStats(double precision) {
		super(precision);
	}

	@Override
	public void handleDocValue(long docValue) {
		newValue(NumericUtils.sortableIntToFloat((int) docValue));
	}
}
