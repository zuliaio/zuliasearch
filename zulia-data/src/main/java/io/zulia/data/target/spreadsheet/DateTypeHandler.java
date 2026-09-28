package io.zulia.data.target.spreadsheet;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;

/**
 * Date handler that also accepts a {@link LocalDate} and a {@link LocalDateTime}. A lambda implements only the Date method and
 * receives a LocalDate as the start of that day in UTC and a LocalDateTime as that wall clock time in UTC. Override the other
 * methods to write a calendar day or a wall clock time natively.
 */
@FunctionalInterface
public interface DateTypeHandler<T> extends SpreadsheetTypeHandler<T, Date> {

	default void writeType(T reference, LocalDate value) {
		writeType(reference, value != null ? Date.from(value.atStartOfDay(ZoneOffset.UTC).toInstant()) : null);
	}

	default void writeType(T reference, LocalDateTime value) {
		writeType(reference, value != null ? Date.from(value.toInstant(ZoneOffset.UTC)) : null);
	}

}
