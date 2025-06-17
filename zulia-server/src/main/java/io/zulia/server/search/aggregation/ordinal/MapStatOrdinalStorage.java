package io.zulia.server.search.aggregation.ordinal;

import com.koloboke.collect.map.IntObjMap;
import com.koloboke.collect.map.hash.HashIntObjMaps;
import io.zulia.message.ZuliaQuery;
import io.zulia.server.search.aggregation.stats.Stats;
import io.zulia.server.search.aggregation.stats.TopStatsQueue;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;

import java.io.IOException;
import java.util.Arrays;
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

		ZuliaQuery.FacetStatsInternal[] facetStats = new ZuliaQuery.FacetStatsInternal[q.size()];
		for (int i = facetStats.length - 1; i >= 0; i--) {
			T stat = q.pop();
			FacetLabel child = taxonomyReader.getPath(stat.getOrdinal());
			String label = child.components[countPath.length];
			facetStats[i] = stat.buildResponse().setFacet(label).build();
		}

		return Arrays.asList(facetStats);
	}

	public void merge(MapStatOrdinalStorage<?> other) {
		for (Map.Entry<Integer, ?> otherOrdinalToStatEntry : other.ordinalToStat.entrySet()) {
			getOrCreateStat(otherOrdinalToStatEntry.getKey()).merge((Stats<?>) otherOrdinalToStatEntry.getValue());
		}
	}

}
