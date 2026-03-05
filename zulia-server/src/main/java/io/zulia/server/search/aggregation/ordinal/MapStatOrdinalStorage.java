package io.zulia.server.search.aggregation.ordinal;

import com.koloboke.collect.map.IntObjMap;
import com.koloboke.collect.map.hash.HashIntObjMaps;
import io.zulia.message.ZuliaQuery;
import io.zulia.server.search.aggregation.stats.Stats;
import io.zulia.server.search.aggregation.stats.TopStatsQueue;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public abstract class MapStatOrdinalStorage<T extends Stats<T>> implements StatOrdinalStorage {

	private final Supplier<T> statConstructor;
	private final IntObjMap<T> ordinalToStat;

	public MapStatOrdinalStorage(Supplier<T> statConstructor) {
		ordinalToStat = HashIntObjMaps.newMutableMap();
		this.statConstructor = statConstructor;
	}

	public T getOrCreateStat(int ordinal) {

		return ordinalToStat.computeIfAbsent(ordinal, i -> {
			T t = statConstructor.get();
			t.setOrdinal(ordinal);
			return t;
		});

	}

	public T getStat(int ordinal) {
		return ordinalToStat.get(ordinal);
	}

	protected abstract TopStatsQueue<T> getTopStatsQueue(TaxonomyReader taxonomyReader, TaxonomyReader.ChildrenIterator childrenIterator, int topN);

	public List<ZuliaQuery.FacetStatsInternal> getFacetStats(TaxonomyReader taxonomyReader, FacetLabel countPath, int topN) throws IOException {
		int dimOrd = taxonomyReader.getOrdinal(countPath);
		if (dimOrd == -1) {
			return null;
		}

		TaxonomyReader.ChildrenIterator childrenIterator = taxonomyReader.getChildren(dimOrd);

		TopStatsQueue<T> q = getTopStatsQueue(taxonomyReader, childrenIterator, topN);

		int qSize = q.size();
		List<T> stats = new ArrayList<>(qSize);
		int[] ords = new int[qSize];
		for (int i = qSize - 1; i >= 0; i--) {
			T stat = q.pop();
			stats.addFirst(stat);
			ords[i] = stat.getOrdinal();
		}

		FacetLabel[] paths = taxonomyReader.getBulkPath(ords);

		List<ZuliaQuery.FacetStatsInternal> facetStats = new ArrayList<>(qSize);
		for (int i = 0; i < qSize; i++) {
			String label = paths[i].components[countPath.length];
			facetStats.add(stats.get(i).buildResponse().setFacet(label).build());
		}

		return facetStats;
	}

	public synchronized void merge(MapStatOrdinalStorage<?> other) {
		for (Map.Entry<Integer, ?> otherOrdinalToStatEntry : other.ordinalToStat.entrySet()) {
			Stats<?> otherValue = (Stats<?>) otherOrdinalToStatEntry.getValue();
			getOrCreateStat(otherOrdinalToStatEntry.getKey()).merge(otherValue);
		}
	}

}
