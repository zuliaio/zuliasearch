package io.zulia.server.search.aggregation.ordinal;

import io.zulia.server.search.aggregation.stats.DoubleStats;
import io.zulia.server.search.aggregation.stats.TopStatsQueue;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;

import java.util.function.Supplier;

public class DoubleMapStatOrdinalStorage extends MapStatOrdinalStorage<DoubleStats> implements DoubleStatOrdinalStorage {
	public DoubleMapStatOrdinalStorage(Supplier<DoubleStats> statConstructor) {
		super(statConstructor);
	}

	@Override
	protected TopStatsQueue<DoubleStats> getTopStatsQueue(TaxonomyReader taxonomyReader, TaxonomyReader.ChildrenIterator childrenIterator, int topN) {
		TopStatsQueue<DoubleStats> q = new TopStatsQueue<>(Math.min(taxonomyReader.getSize(), topN));
		double doubleBottomValue = Double.NEGATIVE_INFINITY;

		int child;
		while ((child = childrenIterator.next()) != TaxonomyReader.INVALID_ORDINAL) {
			DoubleStats stat = getStat(child);
			if (stat != null) {
				if (stat.getDoubleSum() > doubleBottomValue) {
					q.insertWithOverflow(stat);
					if (q.size() == topN) {
						doubleBottomValue = q.top().getDoubleSum();
					}
				}

			}

		}

		return q;
	}

}
