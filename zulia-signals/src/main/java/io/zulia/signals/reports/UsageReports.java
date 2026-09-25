package io.zulia.signals.reports;

import io.zulia.signals.client.SignalsClient;
import io.zulia.signals.storage.SignalField;

import java.time.LocalDate;
import java.time.YearMonth;
import java.util.List;

/** SYSTEM actors are excluded from every report. Counts are exact up to {@link #maxFacetValues()} values per dimension. */
public final class UsageReports {

	public static final int DEFAULT_MAX_FACET_VALUES = 50_000;
	/** A tally by tag runs one search per actor or per value, whichever is fewer. Past this many it throws instead. */
	public static final int DEFAULT_MAX_TALLY_SEARCHES = 1_000;
	// the server requests ten times this many facets per shard
	public static final int MAX_MAX_FACET_VALUES = Integer.MAX_VALUE / 10;

	private final SignalsClient client;
	private final int maxFacetValues;
	private final int maxTallySearches;

	public UsageReports(SignalsClient client) {
		this(client, DEFAULT_MAX_FACET_VALUES, DEFAULT_MAX_TALLY_SEARCHES);
	}

	private UsageReports(SignalsClient client, int maxFacetValues, int maxTallySearches) {
		this.client = client;
		this.maxFacetValues = maxFacetValues;
		this.maxTallySearches = maxTallySearches;
	}

	/** The same reports counting up to this many values per dimension. Every value comes back in one response, so raise it with care. */
	public UsageReports maxFacetValues(int maxFacetValues) {
		if (maxFacetValues < 1 || maxFacetValues > MAX_MAX_FACET_VALUES) {
			throw new IllegalArgumentException("Max facet values must be between 1 and " + MAX_MAX_FACET_VALUES + " but was " + maxFacetValues);
		}
		return new UsageReports(client, maxFacetValues, maxTallySearches);
	}

	public int maxFacetValues() {
		return maxFacetValues;
	}

	/** The same reports allowing a tally by tag to run up to this many searches. */
	public UsageReports maxTallySearches(int maxTallySearches) {
		if (maxTallySearches < 1) {
			throw new IllegalArgumentException("Max tally searches must be at least 1 but was " + maxTallySearches);
		}
		return new UsageReports(client, maxFacetValues, maxTallySearches);
	}

	public int maxTallySearches() {
		return maxTallySearches;
	}

	/** Reports for one app over one range. */
	public UsageReport of(String app, TimeRange range) {
		return new UsageReport(client, app, range, maxFacetValues, maxTallySearches);
	}

	/** Reports for one app from the start of the day in the index zone up to now. */
	public UsageReport of(String app, LocalDate since) {
		return of(app, TimeRange.since(since, client.config().zone(), client.clock()));
	}

	/** Reports for one app over one calendar month in the index zone. */
	public UsageReport of(String app, YearMonth month) {
		return of(app, TimeRange.month(month, client.config().zone()));
	}

	/** Counts per app, the cross app rollup. */
	public List<DimensionCount> byApp(TimeRange range) {
		String field = SignalField.APP.fieldName();
		return UsageReport.counts(UsageReport.rangeSearch(client, range, false, search -> search.addCountFacet(UsageReport.topN(field, maxFacetValues))),
				field);
	}
}
