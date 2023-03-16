package io.zulia.server.search.aggregation.ordinal;

import io.zulia.server.search.aggregation.stats.DoubleStats;

public interface DoubleStatOrdinalStorage extends StatOrdinalStorage {

	@Override
	DoubleStats getOrCreateStat(int ordinal);

	DoubleStats getStat(int ordinal);
}
