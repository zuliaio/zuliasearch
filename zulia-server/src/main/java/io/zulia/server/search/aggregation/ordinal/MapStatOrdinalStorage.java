package io.zulia.server.search.aggregation.ordinal;

import io.zulia.message.ZuliaQuery;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import io.zulia.server.search.aggregation.stats.Stats;
import io.zulia.server.search.aggregation.stats.TopStatsQueue;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;

import io.zulia.server.search.aggregation.AggregationHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.function.IntUnaryOperator;
import java.util.function.Supplier;

public abstract class MapStatOrdinalStorage<T extends Stats<T>> implements StatOrdinalStorage {

	private final Supplier<T> statConstructor;
	private final IntObjectHashMap<T> ordinalToStat;

	public MapStatOrdinalStorage(Supplier<T> statConstructor) {
		ordinalToStat = new IntObjectHashMap<>();
		this.statConstructor = statConstructor;
	}

	public T getOrCreateStat(int ordinal) {
		return ordinalToStat.getIfAbsentPut(ordinal, () -> {
			T t = statConstructor.get();
			t.setOrdinal(ordinal);
			return t;
		});
	}

	public T getStat(int ordinal) {
		return ordinalToStat.get(ordinal);
	}

	protected abstract TopStatsQueue<T> getTopStatsQueue(TaxonomyReader taxonomyReader, TaxonomyReader.ChildrenIterator childrenIterator, int topN,
			int dimChildCount);

	private List<T> collectAndSort(TaxonomyReader.ChildrenIterator childrenIterator, int topN) {
		List<T> collected = new ArrayList<>();
		int child;
		while ((child = childrenIterator.next()) != TaxonomyReader.INVALID_ORDINAL) {
			T stat = getStat(child);
			if (stat != null) {
				collected.add(stat);
			}
		}
		collected.sort(Comparator.reverseOrder());
		if (collected.size() > topN) {
			return collected.subList(0, topN);
		}
		return collected;
	}

	public List<ZuliaQuery.FacetStatsInternal> getFacetStats(TaxonomyReader taxonomyReader, FacetLabel countPath, int topN,
			IntUnaryOperator dimensionChildCount) throws IOException {
		int dimOrd = taxonomyReader.getOrdinal(countPath);
		if (dimOrd == -1) {
			return null;
		}

		int dimChildCount = dimensionChildCount.applyAsInt(dimOrd);

		List<T> stats;
		int[] ords;

		if (AggregationHandler.shouldCollectAndSort(topN, dimChildCount)) {
			TaxonomyReader.ChildrenIterator childrenIterator = taxonomyReader.getChildren(dimOrd);
			stats = collectAndSort(childrenIterator, topN);
			ords = new int[stats.size()];
			for (int i = 0; i < stats.size(); i++) {
				ords[i] = stats.get(i).getOrdinal();
			}
		}
		else {
			TaxonomyReader.ChildrenIterator childrenIterator = taxonomyReader.getChildren(dimOrd);
			TopStatsQueue<T> q = getTopStatsQueue(taxonomyReader, childrenIterator, topN, dimChildCount);

			int qSize = q.size();
			stats = new ArrayList<>(qSize);
			ords = new int[qSize];
			for (int i = qSize - 1; i >= 0; i--) {
				T stat = q.pop();
				stats.addFirst(stat);
				ords[i] = stat.getOrdinal();
			}
		}

		FacetLabel[] paths = taxonomyReader.getBulkPath(ords);

		List<ZuliaQuery.FacetStatsInternal> facetStats = new ArrayList<>(stats.size());
		for (int i = 0; i < stats.size(); i++) {
			String label = paths[i].components[countPath.length];
			facetStats.add(stats.get(i).buildResponse().setFacet(label).build());
		}

		return facetStats;
	}

	public synchronized void merge(MapStatOrdinalStorage<?> other) {
		other.ordinalToStat.forEachKeyValue((key, value) -> getOrCreateStat(key).merge(value));
	}

}
