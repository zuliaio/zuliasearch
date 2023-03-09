package io.zulia.server.search.aggregation.ordinal;

import io.zulia.server.search.aggregation.stats.LongStats;
import io.zulia.server.search.aggregation.stats.TopStatsQueue;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;

import java.util.function.Supplier;

public class LongMapStatOrdinalStorage extends MapStatOrdinalStorage<LongStats> implements LongStatOrdinalStorage {
	public LongMapStatOrdinalStorage(Supplier<LongStats> statConstructor) {
		super(statConstructor);
	}

	@Override
	protected TopStatsQueue<LongStats> getTopStatsQueue(TaxonomyReader taxonomyReader, TaxonomyReader.ChildrenIterator childrenIterator, int topN) {
		TopStatsQueue<LongStats> q = new TopStatsQueue<>(Math.min(taxonomyReader.getSize(), topN));
		long longBottomValue = Long.MIN_VALUE;
		int child;
		while ((child = childrenIterator.next()) != TaxonomyReader.INVALID_ORDINAL) {
			LongStats stat = getStat(child);
			if (stat != null) {
				if (stat.getLongSum() > longBottomValue) {
					q.insertWithOverflow(stat);
					if (q.size() == topN) {
						longBottomValue = q.top().getLongSum();
					}
				}
			}

		}
		return q;
	}
}
