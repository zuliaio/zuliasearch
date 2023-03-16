package io.zulia.server.search.aggregation.stats;

import org.apache.lucene.util.NumericUtils;

public class DoubleDoubleStats extends DoubleStats {
	public DoubleDoubleStats(double precision) {
		super(precision);
	}

	@Override
	public void handleDocValue(long docValue) {
		newValue(NumericUtils.sortableLongToDouble(docValue));
	}
}
