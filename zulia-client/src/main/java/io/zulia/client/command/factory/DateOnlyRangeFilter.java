package io.zulia.client.command.factory;

import java.time.format.DateTimeFormatter;
import java.util.Date;

public class DateOnlyRangeFilter extends RangeFilter<Date> {

    public DateOnlyRangeFilter(String field) {
        super(field);
    }

    @Override
    public String getAsString(Date val) {
        return DateTimeFormatter.ISO_LOCAL_DATE.format(val.toInstant());
    }
}
