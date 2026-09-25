package io.zulia.signals.reports;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.ZoneId;

/** from inclusive, to exclusive. */
public record TimeRange(Instant from, Instant to) {

	public TimeRange {
		if (from == null || to == null) {
			throw new IllegalArgumentException("Time range needs both ends, got from " + from + " to " + to);
		}
		if (!from.isBefore(to)) {
			throw new IllegalArgumentException("Time range from must be before to, got from " + from + " to " + to);
		}
	}

	public static TimeRange lastDays(int days) {
		return lastDays(days, Clock.systemUTC());
	}

	public static TimeRange lastDays(int days, Clock clock) {
		if (days < 1) {
			throw new IllegalArgumentException("Days must be at least 1, got " + days);
		}
		Instant now = Instant.now(clock);
		return new TimeRange(now.minus(Duration.ofDays(days)), now);
	}

	/** The start of the day in the zone up to now. */
	public static TimeRange since(LocalDate from, ZoneId zone) {
		return since(from, zone, Clock.systemUTC());
	}

	public static TimeRange since(LocalDate from, ZoneId zone, Clock clock) {
		requireDate(from, "Since date");
		requireZone(zone);
		Instant start = from.atStartOfDay(zone).toInstant();
		Instant now = Instant.now(clock);
		if (!start.isBefore(now)) {
			throw new IllegalArgumentException("Since date " + from + " starts at " + start + " in " + zone + ", which is not before now " + now);
		}
		return new TimeRange(start, now);
	}

	/** Whole days in the zone, the last day included. */
	public static TimeRange days(LocalDate from, LocalDate toInclusive, ZoneId zone) {
		requireDate(from, "From date");
		requireDate(toInclusive, "To date");
		requireZone(zone);
		if (toInclusive.isBefore(from)) {
			throw new IllegalArgumentException("To date " + toInclusive + " is before from date " + from);
		}
		return new TimeRange(from.atStartOfDay(zone).toInstant(), toInclusive.plusDays(1).atStartOfDay(zone).toInstant());
	}

	/** One calendar month in the zone. */
	public static TimeRange month(YearMonth month, ZoneId zone) {
		if (month == null) {
			throw new IllegalArgumentException("Month is required but was null");
		}
		requireZone(zone);
		return new TimeRange(month.atDay(1).atStartOfDay(zone).toInstant(), month.plusMonths(1).atDay(1).atStartOfDay(zone).toInstant());
	}

	private static void requireDate(LocalDate date, String what) {
		if (date == null) {
			throw new IllegalArgumentException(what + " is required but was null");
		}
	}

	private static void requireZone(ZoneId zone) {
		if (zone == null) {
			throw new IllegalArgumentException("Zone is required but was null, the SignalsIndexConfig zone is the usual choice");
		}
	}
}
