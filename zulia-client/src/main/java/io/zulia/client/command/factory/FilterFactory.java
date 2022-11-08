package io.zulia.client.command.factory;

/**
 * Factory class for producing types instances of RangeFilters
 */
public class FilterFactory {
	public static RangeFilter<Double> rangeDouble(String fieldName) {
		return new RangeFilter<>(fieldName);
	}

	public static RangeFilter<Long> rangeLong(String fieldName) {
		return new RangeFilter<>(fieldName);
	}

	public static RangeFilter<Integer> rangeInt(String fieldName) {
		return new RangeFilter<>(fieldName);
	}
}
