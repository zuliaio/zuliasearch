package io.zulia.signals.reports;

import io.zulia.signals.client.SignalsClient;
import io.zulia.signals.storage.SignalField;

import java.util.List;

/** SYSTEM actors are excluded from every report. Counts are exact up to {@link #MAX_FACET_VALUES} values per dimension. */
public final class UsageReports {

	public static final int MAX_FACET_VALUES = 10_000;

	private final SignalsClient client;

	public UsageReports(SignalsClient client) {
		this.client = client;
	}

	/** Reports for one app over one range. */
	public UsageReport of(String app, TimeRange range) {
		return new UsageReport(client, app, range);
	}

	/** Counts per app, the cross app rollup. */
	public List<DimensionCount> byApp(TimeRange range) {
		String field = SignalField.APP.fieldName();
		return UsageReport.counts(UsageReport.rangeSearch(client, range, false, search -> search.addCountFacet(UsageReport.topN(field))), field);
	}
}
