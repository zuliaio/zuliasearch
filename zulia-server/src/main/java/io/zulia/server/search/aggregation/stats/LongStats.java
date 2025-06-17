package io.zulia.server.search.aggregation.stats;

import io.zulia.message.ZuliaQuery;

public abstract class LongStats extends Stats<LongStats> implements Comparable<LongStats> {
	private long longSum;
	private long longMinValue = Long.MAX_VALUE;
	private long longMaxValue = Long.MIN_VALUE;

	public LongStats(double precision) {
		super(precision);
	}

	public void newValue(long newValue) {
		this.longSum += newValue;
		if (newValue < longMinValue) {
			longMinValue = newValue;
		}
		if (newValue > longMaxValue) {
			longMaxValue = newValue;
		}
		tallyValue(newValue);
	}

	public long getLongSum() {
		return longSum;
	}

	@Override
	public ZuliaQuery.FacetStatsInternal.Builder buildResponse() {
		ZuliaQuery.FacetStatsInternal.Builder builder = super.buildResponse();
		ZuliaQuery.SortValue sum = ZuliaQuery.SortValue.newBuilder().setLongValue(longSum).build();
		ZuliaQuery.SortValue min = ZuliaQuery.SortValue.newBuilder().setLongValue(longMinValue).build();
		ZuliaQuery.SortValue max = ZuliaQuery.SortValue.newBuilder().setLongValue(longMaxValue).build();
		return builder.setSum(sum).setMin(min).setMax(max);
	}

	public int compareTo(LongStats o) {
		int compare = Double.compare(longSum, o.longSum);
		if (compare == 0) {
			return compareOrdinal(o);
		}
		return compare;
	}

	public void mergeExtra(LongStats other) {
		this.longSum += other.longSum;
		this.longMinValue = Math.min(this.longMinValue, other.longMinValue);
		this.longMaxValue = Math.max(this.longMaxValue, other.longMaxValue);
	}
}