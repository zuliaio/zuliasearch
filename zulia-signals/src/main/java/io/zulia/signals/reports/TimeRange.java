package io.zulia.signals.reports;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

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
}
