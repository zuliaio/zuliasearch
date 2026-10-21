package io.zulia.server.search.distinct;

import org.apache.lucene.search.Collector;
import org.apache.lucene.search.ScoreMode;

/**
 * Collects the distinct values of one sortable field for one search slice. Slices merge into one when the search completes.
 */
public sealed interface DistinctValueCollector extends Collector permits StringDistinctValueCollector, NumericDistinctValueCollector {

	void merge(DistinctValueCollector other);

	@Override
	default ScoreMode scoreMode() {
		return ScoreMode.COMPLETE_NO_SCORES;
	}
}
