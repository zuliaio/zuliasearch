package io.zulia.server.search.aggregation.ordinal;

import io.zulia.server.search.aggregation.stats.LongStats;

public interface LongStatOrdinalStorage extends StatOrdinalStorage {

	@Override
	LongStats getOrCreateStat(int ordinal);

	LongStats getStat(int ordinal);
}
