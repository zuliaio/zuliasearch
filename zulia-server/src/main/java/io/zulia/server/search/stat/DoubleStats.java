package io.zulia.server.search.stat;

import io.zulia.message.ZuliaQuery;

public abstract class DoubleStats extends Stats<DoubleStats> implements Comparable<DoubleStats> {
	private double doubleSum;
	private double doubleMinValue = Double.POSITIVE_INFINITY;
	private double doubleMaxValue = Double.NEGATIVE_INFINITY;

	public DoubleStats(double precision) {
		super(precision);
	}

	public void newValue(double newValue) {
		this.doubleSum += newValue;
		if (newValue < doubleMinValue) {
			doubleMinValue = newValue;
		}
		if (newValue > doubleMaxValue) {
			doubleMaxValue = newValue;
		}

		tallyValue(newValue);
	}

	public double getDoubleSum() {
		return doubleSum;
	}

	public ZuliaQuery.FacetStatsInternal.Builder buildResponse() {
		ZuliaQuery.FacetStatsInternal.Builder builder = super.buildResponse();
		ZuliaQuery.SortValue sum = ZuliaQuery.SortValue.newBuilder().setDoubleValue(doubleSum).build();
		ZuliaQuery.SortValue min = ZuliaQuery.SortValue.newBuilder().setDoubleValue(doubleMinValue).build();
		ZuliaQuery.SortValue max = ZuliaQuery.SortValue.newBuilder().setDoubleValue(doubleMaxValue).build();
		return builder.setSum(sum).setMin(min).setMax(max);
	}

	@Override
	public int compareTo(DoubleStats o) {
		int compare = Double.compare(doubleSum, o.doubleSum);
		if (compare == 0) {
			return Integer.compare(o.ordinal, ordinal);
		}
		return compare;
	}

}