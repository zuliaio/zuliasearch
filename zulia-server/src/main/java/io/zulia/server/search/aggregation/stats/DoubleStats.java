package io.zulia.server.search.aggregation.stats;

import com.google.common.util.concurrent.AtomicDouble;
import io.zulia.message.ZuliaQuery;

public abstract class DoubleStats extends Stats<DoubleStats> implements Comparable<DoubleStats> {
	private final AtomicDouble doubleSum;
	private final AtomicDouble doubleMinValue;
	private final AtomicDouble doubleMaxValue;

	public DoubleStats(double precision) {
		super(precision);
		doubleSum = new AtomicDouble();
		doubleMinValue = new AtomicDouble(Double.POSITIVE_INFINITY);
		doubleMaxValue = new AtomicDouble(Double.NEGATIVE_INFINITY);
	}

	public void newValue(double newValue) {
		doubleSum.addAndGet(newValue);
		doubleMinValue.getAndAccumulate(newValue, Math::min);
		doubleMaxValue.getAndAccumulate(newValue, Math::max);

		tallyValue(newValue);
	}

	public double getDoubleSum() {
		return doubleSum.get();
	}

	public ZuliaQuery.FacetStatsInternal.Builder buildResponse() {
		ZuliaQuery.FacetStatsInternal.Builder builder = super.buildResponse();
		ZuliaQuery.SortValue sum = ZuliaQuery.SortValue.newBuilder().setDoubleValue(doubleSum.get()).build();
		ZuliaQuery.SortValue min = ZuliaQuery.SortValue.newBuilder().setDoubleValue(doubleMinValue.get()).build();
		ZuliaQuery.SortValue max = ZuliaQuery.SortValue.newBuilder().setDoubleValue(doubleMaxValue.get()).build();
		return builder.setSum(sum).setMin(min).setMax(max);
	}

	@Override
	public int compareTo(DoubleStats o) {
		int compare = Double.compare(doubleSum.get(), o.doubleSum.get());
		if (compare == 0) {
			return Integer.compare(o.ordinal, ordinal);
		}
		return compare;
	}

}