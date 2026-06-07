package io.zulia.client.command.factory;

import io.zulia.client.command.builder.FilterQuery;

/**
 * Templated class to handle range filters. Generates the appropriate query to populate a FilterQuery object
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
	 * Builds the range query string without any exclusion, e.g. {@code field:[min TO max]}. The bracket style reflects
	 * the endpoint behavior and a missing endpoint is written as {@code *}.
	 *
	 * @return the base range query string
	 */
	public String getBaseQuery() {
		String minString = minValue == null ? "*" : getAsString(minValue);
		String maxString = maxValue == null ? "*" : getAsString(maxValue);

		// default to inclusive endpoints and flip to exclusive '{' / '}' per the endpoint behavior
		char open = (minValue != null && (behavior == RangeBehavior.EXCLUSIVE || behavior == RangeBehavior.INCLUDE_MAX)) ? '{' : '[';
		char close = (maxValue != null && (behavior == RangeBehavior.EXCLUSIVE || behavior == RangeBehavior.INCLUDE_MIN)) ? '}' : ']';

		return field + ":" + open + minString + " TO " + maxString + close;
	}

	/**
	 * Builds the full range query string, prefixing {@code -} when the range is excluded.
	 *
	 * @return the range query string, negated with a leading {@code -} when excluded
	 */
	public String toQueryString() {
		return exclude ? "-" + getBaseQuery() : getBaseQuery();
	}

	/**
	 * Convert contents to a FilterQuery for searching. Exclusion is applied as a FILTER_NOT query type rather than a
	 * leading {@code -} on the query string.
	 *
	 * @return Filter Query matching these requirements
	 */
	public FilterQuery toQuery() {
		FilterQuery query = new FilterQuery(getBaseQuery());

		if (exclude) {
			query.exclude();
		}

		return query;
	}

	public String getAsString(T val) {
		return val.toString();
	}
}
