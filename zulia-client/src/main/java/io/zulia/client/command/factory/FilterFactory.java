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

	public static RangeFilter<String> rangeString(String fieldName) {
		return new RangeFilter<>(fieldName);
	}

	public static InstantRangeFilter rangeInstant(String fieldName) {
		return new InstantRangeFilter(fieldName);
	}

	public static LocalDateRangeFilter rangeLocalDate(String fieldName) {
		return new LocalDateRangeFilter(fieldName);
	}

	public static DateOnlyRangeFilter rangeDate(String fieldName) {
		return new DateOnlyRangeFilter(fieldName);
	}

	public static DateWithTimeRangeFilter rangeDateWithTime(String fieldName) {
		return new DateWithTimeRangeFilter(fieldName);
	}

}
