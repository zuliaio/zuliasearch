package io.zulia.server.search.aggregation.stats;

import io.zulia.message.ZuliaQuery;

import java.util.concurrent.atomic.AtomicLong;

public abstract class LongStats extends Stats<LongStats> implements Comparable<LongStats> {
	private final AtomicLong longSum;
	private final AtomicLong longMinValue;
	private final AtomicLong longMaxValue;

	public LongStats(double precision) {
		super(precision);
		longSum = new AtomicLong();
		longMinValue = new AtomicLong(Long.MAX_VALUE);
		longMaxValue = new AtomicLong(Long.MIN_VALUE);
	}

	public void newValue(long newValue) {
		longSum.addAndGet(newValue);
		longMinValue.getAndAccumulate(newValue, Math::min);
		longMaxValue.getAndAccumulate(newValue, Math::max);
		tallyValue(newValue);
	}

	public long getLongSum() {
		return longSum.get();
	}

	@Override
	public ZuliaQuery.FacetStatsInternal.Builder buildResponse() {
		ZuliaQuery.FacetStatsInternal.Builder builder = super.buildResponse();
		ZuliaQuery.SortValue sum = ZuliaQuery.SortValue.newBuilder().setLongValue(longSum.get()).build();
		ZuliaQuery.SortValue min = ZuliaQuery.SortValue.newBuilder().setLongValue(longMinValue.get()).build();
		ZuliaQuery.SortValue max = ZuliaQuery.SortValue.newBuilder().setLongValue(longMaxValue.get()).build();
		return builder.setSum(sum).setMin(min).setMax(max);
	}

	public int compareTo(LongStats o) {
		int compare = Double.compare(longSum.get(), o.longSum.get());
		if (compare == 0) {
			return Integer.compare(o.ordinal, ordinal);
		}
		return compare;
	}

}