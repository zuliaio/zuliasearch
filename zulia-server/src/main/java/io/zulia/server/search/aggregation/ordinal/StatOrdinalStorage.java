package io.zulia.server.search.aggregation.ordinal;

import io.zulia.server.search.aggregation.stats.Stats;


public interface StatOrdinalStorage {

	Stats<?> getOrCreateStat(int ordinal);

	Stats<?> getStat(int ordinal);

}
