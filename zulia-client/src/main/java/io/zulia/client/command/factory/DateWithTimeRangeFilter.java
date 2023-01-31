package io.zulia.client.command.factory;

import java.time.format.DateTimeFormatter;
import java.util.Date;

public class DateWithTimeRangeFilter extends RangeFilter<Date> {

    public DateWithTimeRangeFilter(String field) {
        super(field);
    }

    @Override
    public String getAsString(Date val) {
        return DateTimeFormatter.ISO_INSTANT.format(val.toInstant());
    }
}
