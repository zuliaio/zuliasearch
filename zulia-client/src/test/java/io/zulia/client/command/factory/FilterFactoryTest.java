package io.zulia.client.command.factory;

import io.zulia.message.ZuliaQuery;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Date;

public class FilterFactoryTest {

	@Test
	public void rangeDate() {
		// a Date carries a time; the calendar date is taken in UTC, so a non-midnight instant still maps to its UTC day
		Date start = Date.from(LocalDate.of(2014, 10, 5).atTime(23, 30).atZone(ZoneOffset.UTC).toInstant());
		Date end = Date.from(LocalDate.of(2014, 12, 31).atStartOfDay(ZoneOffset.UTC).toInstant());

		// base query string: brackets reflect endpoint behavior, * marks a missing endpoint, no exclusion
		Assertions.assertEquals("date:[2014-10-05 TO 2014-12-31]", FilterFactory.rangeDate("date").setRange(start, end).getBaseQuery());
		Assertions.assertEquals("date:[2014-10-05 TO *]", FilterFactory.rangeDate("date").setMinValue(start).getBaseQuery());
		Assertions.assertEquals("date:[* TO 2014-12-31]", FilterFactory.rangeDate("date").setMaxValue(end).getBaseQuery());
		Assertions.assertEquals("date:{2014-10-05 TO 2014-12-31}",
				FilterFactory.rangeDate("date").setRange(start, end).setEndpointBehavior(RangeBehavior.EXCLUSIVE).getBaseQuery());
		Assertions.assertEquals("date:[2014-10-05 TO 2014-12-31}",
				FilterFactory.rangeDate("date").setRange(start, end).setEndpointBehavior(RangeBehavior.INCLUDE_MIN).getBaseQuery());
		Assertions.assertEquals("date:{2014-10-05 TO 2014-12-31]",
				FilterFactory.rangeDate("date").setRange(start, end).setEndpointBehavior(RangeBehavior.INCLUDE_MAX).getBaseQuery());

		// toQueryString prefixes - when excluded; the base query is never negated
		Assertions.assertEquals("date:[2014-10-05 TO 2014-12-31]", FilterFactory.rangeDate("date").setRange(start, end).toQueryString());
		Assertions.assertEquals("-date:[2014-10-05 TO 2014-12-31]", FilterFactory.rangeDate("date").setRange(start, end).setExclude().toQueryString());

		// toQuery uses the base query string and applies exclusion as the FILTER_NOT query type
		ZuliaQuery.Query included = FilterFactory.rangeDate("date").setRange(start, end).toQuery().getQuery();
		Assertions.assertEquals("date:[2014-10-05 TO 2014-12-31]", included.getQ());
		Assertions.assertEquals(ZuliaQuery.Query.QueryType.FILTER, included.getQueryType());

		ZuliaQuery.Query excluded = FilterFactory.rangeDate("date").setRange(start, end).setExclude().toQuery().getQuery();
		Assertions.assertEquals("date:[2014-10-05 TO 2014-12-31]", excluded.getQ());
		Assertions.assertEquals(ZuliaQuery.Query.QueryType.FILTER_NOT, excluded.getQueryType());
	}

	@Test
	public void rangeLocalDate() {
		LocalDate start = LocalDate.of(2014, 1, 1);
		LocalDate end = LocalDate.of(2014, 12, 31);

		// base query string: brackets reflect endpoint behavior, * marks a missing endpoint, no exclusion
		Assertions.assertEquals("date:[2014-01-01 TO 2014-12-31]", FilterFactory.rangeLocalDate("date").setRange(start, end).getBaseQuery());
		Assertions.assertEquals("date:[2014-10-05 TO *]", FilterFactory.rangeLocalDate("date").setMinValue(LocalDate.of(2014, 10, 5)).getBaseQuery());
		Assertions.assertEquals("date:[* TO 2014-10-05]", FilterFactory.rangeLocalDate("date").setMaxValue(LocalDate.of(2014, 10, 5)).getBaseQuery());
		Assertions.assertEquals("date:{2014-01-01 TO 2014-12-31}",
				FilterFactory.rangeLocalDate("date").setRange(start, end).setEndpointBehavior(RangeBehavior.EXCLUSIVE).getBaseQuery());
		Assertions.assertEquals("date:[2014-01-01 TO 2014-12-31}",
				FilterFactory.rangeLocalDate("date").setRange(start, end).setEndpointBehavior(RangeBehavior.INCLUDE_MIN).getBaseQuery());
		Assertions.assertEquals("date:{2014-01-01 TO 2014-12-31]",
				FilterFactory.rangeLocalDate("date").setRange(start, end).setEndpointBehavior(RangeBehavior.INCLUDE_MAX).getBaseQuery());

		// toQueryString prefixes - when excluded; the base query is never negated
		Assertions.assertEquals("date:[2014-01-01 TO 2014-12-31]", FilterFactory.rangeLocalDate("date").setRange(start, end).toQueryString());
		Assertions.assertEquals("-date:[2014-01-01 TO 2014-12-31]", FilterFactory.rangeLocalDate("date").setRange(start, end).setExclude().toQueryString());

		// toQuery uses the base query string and applies exclusion as the FILTER_NOT query type
		ZuliaQuery.Query included = FilterFactory.rangeLocalDate("date").setRange(start, end).toQuery().getQuery();
		Assertions.assertEquals("date:[2014-01-01 TO 2014-12-31]", included.getQ());
		Assertions.assertEquals(ZuliaQuery.Query.QueryType.FILTER, included.getQueryType());

		ZuliaQuery.Query excluded = FilterFactory.rangeLocalDate("date").setRange(start, end).setExclude().toQuery().getQuery();
		Assertions.assertEquals("date:[2014-01-01 TO 2014-12-31]", excluded.getQ());
		Assertions.assertEquals(ZuliaQuery.Query.QueryType.FILTER_NOT, excluded.getQueryType());
	}
}
