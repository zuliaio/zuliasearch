package io.zulia.client.command.factory;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

public class LocalDateRangeFilter extends RangeFilter<LocalDate> {
    public LocalDateRangeFilter(String field) {
        super(field);
    }

    @Override
    public String getAsString(LocalDate val) {
        return DateTimeFormatter.ISO_LOCAL_DATE.format(val);
    }
}
