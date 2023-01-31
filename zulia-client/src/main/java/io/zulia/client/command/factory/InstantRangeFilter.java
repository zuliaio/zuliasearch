package io.zulia.client.command.factory;

import java.time.Instant;
import java.time.format.DateTimeFormatter;

public class InstantRangeFilter extends RangeFilter<Instant> {
    public InstantRangeFilter(String field) {
        super(field);
    }

    @Override
    public String getAsString(Instant val) {
        return DateTimeFormatter.ISO_INSTANT.format(val);
    }
}
