package io.zulia.signals.reports;

import io.zulia.signals.client.SignalsClient;
import io.zulia.signals.storage.SignalField;

import java.util.List;

/** SYSTEM actors are excluded from every report. Counts are exact up to {@link #maxFacetValues()} values per dimension. */
public final class UsageReports {

	public static final int DEFAULT_MAX_FACET_VALUES = 50_000;
	// the server requests ten times this many facets per shard
	public static final int MAX_MAX_FACET_VALUES = Integer.MAX_VALUE / 10;

	private final SignalsClient client;
	private final int maxFacetValues;

	public UsageReports(SignalsClient client) {
		this(client, DEFAULT_MAX_FACET_VALUES);
	}

	private UsageReports(SignalsClient client, int maxFacetValues) {
		this.client = client;
		this.maxFacetValues = maxFacetValues;
	}

	/** The same reports counting up to this many values per dimension. Every value comes back in one response, so raise it with care. */
	public UsageReports maxFacetValues(int maxFacetValues) {
		if (maxFacetValues < 1 || maxFacetValues > MAX_MAX_FACET_VALUES) {
			throw new IllegalArgumentException("Max facet values must be between 1 and " + MAX_MAX_FACET_VALUES + " but was " + maxFacetValues);
		}
		return new UsageReports(client, maxFacetValues);
	}

	public int maxFacetValues() {
		return maxFacetValues;
	}

	/** Reports for one app over one range. */
	public UsageReport of(String app, TimeRange range) {
		return new UsageReport(client, app, range, maxFacetValues);
	}

	/** Counts per app, the cross app rollup. */
	public List<DimensionCount> byApp(TimeRange range) {
		String field = SignalField.APP.fieldName();
		return UsageReport.counts(UsageReport.rangeSearch(client, range, false, search -> search.addCountFacet(UsageReport.topN(field, maxFacetValues))),
				field);
	}
}
