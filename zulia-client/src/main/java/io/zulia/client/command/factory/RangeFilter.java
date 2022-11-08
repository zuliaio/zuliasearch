package io.zulia.client.command.factory;

import io.zulia.client.command.builder.FilterQuery;

/**
 * Templated class to handle range filters. Generates appropriate query to populate a FilterQuery object
 *
 * @param <T> data type of value to filter on
 */
public class RangeFilter<T> {
	private final String field;
	private boolean exclude;
	private T minValue;
	private T maxValue;
	private RangeBehavior behavior;

	public RangeFilter(String field) {
		this.field = field;
		this.exclude = false;
		this.behavior = RangeBehavior.INCLUSIVE;
		this.minValue = null;
		this.maxValue = null;
	}

	public RangeFilter<T> setExclude() {
		this.exclude = true;
		return this;
	}

	public RangeFilter<T> setExclude(boolean exclude) {
		this.exclude = exclude;
		return this;
	}

	public RangeFilter<T> setMinValue(T minValue) {
		this.minValue = minValue;
		return this;
	}

	public RangeFilter<T> setMaxValue(T maxValue) {
		this.maxValue = maxValue;
		return this;
	}

	public RangeFilter<T> setRange(T minValue, T maxValue) {
		this.minValue = minValue;
		this.maxValue = maxValue;
		return this;
	}

	public RangeFilter<T> setEndpointBehavior(RangeBehavior behavior) {
		this.behavior = behavior;
		return this;
	}

	/**
	 * Convert contents to string for query
	 *
	 * @return Filter Query matching these requirements
	 */
	public FilterQuery toQuery() {
		String minString = minValue == null ? "*" : getAsString(minValue);
		String maxString = maxValue == null ? "*" : getAsString(maxValue);
		char open = '[';
		char close = ']';

		// Build endpoints if applicable
		if (minValue != null && (behavior == RangeBehavior.EXCLUSIVE || behavior == RangeBehavior.INCLUDE_MAX)) {
			open = '{';
		}
		// Build endpoints if applicable
		if (maxValue != null && (behavior == RangeBehavior.EXCLUSIVE || behavior == RangeBehavior.INCLUDE_MIN)) {
			close = '}';
		}
		FilterQuery query = new FilterQuery(field + ":" + open + minString + " TO " + maxString + close);

		if (exclude) {
			query.exclude();
		}

		return query;
	}

	public String getAsString(T val) {
		return val.toString();
	}
}
