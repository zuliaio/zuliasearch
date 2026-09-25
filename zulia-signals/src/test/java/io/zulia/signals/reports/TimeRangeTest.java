package io.zulia.signals.reports;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.ZoneOffset;

class TimeRangeTest {

	private static final ZoneId NEW_YORK = ZoneId.of("America/New_York");

	@Test
	void sinceRunsFromTheZonedDayStartToNow() {
		Instant now = Instant.parse("2026-09-24T15:16:28Z");
		TimeRange range = TimeRange.since(LocalDate.of(2026, 1, 1), NEW_YORK, Clock.fixed(now, ZoneOffset.UTC));
		Assertions.assertEquals(Instant.parse("2026-01-01T05:00:00Z"), range.from(), "midnight in New York");
		Assertions.assertEquals(now, range.to(), "today is inside the range");
		Assertions.assertThrows(IllegalArgumentException.class, () -> TimeRange.since(null, NEW_YORK));
		Assertions.assertThrows(IllegalArgumentException.class, () -> TimeRange.since(LocalDate.of(2026, 1, 1), null));
		IllegalArgumentException future = Assertions.assertThrows(IllegalArgumentException.class,
				() -> TimeRange.since(LocalDate.of(2027, 1, 1), NEW_YORK, Clock.fixed(now, ZoneOffset.UTC)));
		Assertions.assertTrue(future.getMessage().contains("2027-01-01") && future.getMessage().contains("America/New_York"), future.getMessage());
	}

	@Test
	void daysIncludeTheLastDay() {
		TimeRange range = TimeRange.days(LocalDate.of(2026, 9, 1), LocalDate.of(2026, 9, 30), NEW_YORK);
		Assertions.assertEquals(Instant.parse("2026-09-01T04:00:00Z"), range.from());
		Assertions.assertEquals(Instant.parse("2026-10-01T04:00:00Z"), range.to(), "exclusive end is the day after the last day");
		Assertions.assertEquals(TimeRange.days(LocalDate.of(2026, 9, 1), LocalDate.of(2026, 9, 1), NEW_YORK).to(),
				Instant.parse("2026-09-02T04:00:00Z"), "a single day");
		Assertions.assertThrows(IllegalArgumentException.class, () -> TimeRange.days(LocalDate.of(2026, 9, 2), LocalDate.of(2026, 9, 1), NEW_YORK), "reversed");
		TimeRange march = TimeRange.days(LocalDate.of(2026, 3, 1), LocalDate.of(2026, 3, 31), NEW_YORK);
		Assertions.assertEquals(Instant.parse("2026-03-01T05:00:00Z"), march.from(), "standard time before the spring change");
		Assertions.assertEquals(Instant.parse("2026-04-01T04:00:00Z"), march.to(), "daylight time after it");
	}

	@Test
	void monthCoversTheCalendarMonth() {
		TimeRange range = TimeRange.month(YearMonth.of(2026, 2), ZoneOffset.UTC);
		Assertions.assertEquals(Instant.parse("2026-02-01T00:00:00Z"), range.from());
		Assertions.assertEquals(Instant.parse("2026-03-01T00:00:00Z"), range.to());
		Assertions.assertEquals(TimeRange.days(LocalDate.of(2026, 2, 1), LocalDate.of(2026, 2, 28), NEW_YORK), TimeRange.month(YearMonth.of(2026, 2), NEW_YORK),
				"a month is its days");
		Assertions.assertThrows(IllegalArgumentException.class, () -> TimeRange.month(null, NEW_YORK));
	}
}
