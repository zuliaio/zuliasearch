package io.zulia.util;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class ZuliaDateUtil {
	public static Long getDateAsLong(String dateString) {
		try {
			if (dateString.contains(":")) {
				return Instant.parse(dateString).toEpochMilli();
			}
			else {
				LocalDate parse = LocalDate.parse(dateString, DateTimeFormatter.ISO_LOCAL_DATE);
				return parse.atStartOfDay().toInstant(ZoneOffset.UTC).toEpochMilli();
			}
		}
		catch (Exception e) {
			return null;
		}
	}
}
