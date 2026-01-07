package io.zulia.server.search.aggregation.facets;

import com.koloboke.collect.map.hash.HashObjIntMap;
import com.koloboke.collect.map.hash.HashObjIntMaps;
import com.koloboke.collect.map.hash.HashObjLongMap;
import com.koloboke.collect.map.hash.HashObjLongMaps;
import io.zulia.message.ZuliaQuery;
import io.zulia.message.ZuliaQuery.FacetCount;
import io.zulia.message.ZuliaQuery.FacetGroup;
import org.apache.lucene.util.FixedBitSet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class FacetCombiner {

	public record FacetGroupWithShardIndex(FacetGroup facetGroup, int shardIndex) {

	}

	private final List<FacetGroupWithShardIndex> facetGroups;
	private final int[] shardIndexes;
	private final ZuliaQuery.CountRequest countRequest;
	private final int shardReponses;

	public FacetCombiner(ZuliaQuery.CountRequest countRequest, int shardReponses) {
		this.countRequest = countRequest;
		this.shardReponses = shardReponses;
		this.facetGroups = new ArrayList<>(shardReponses);
		this.shardIndexes = new int[shardReponses];
	}

	public void handleFacetGroupForShard(FacetGroup facetGroup, int shardIndex) {
		facetGroups.add(new FacetGroupWithShardIndex(facetGroup, shardIndex));
	}

	public FacetGroup getCombinedFacetGroup() {
		if (facetGroups.size() == 1) {
			return facetGroups.getFirst().facetGroup();
		}
		else {

			HashObjLongMap<String> facetCounts = HashObjLongMaps.newMutableMap();
			Map<String, FixedBitSet> shardsReturned = new HashMap<>();
			FixedBitSet fullResults = new FixedBitSet(shardReponses);
			long[] minForShard = new long[shardReponses];

			for (FacetGroupWithShardIndex facetGroupWithShardIndex : facetGroups) {
				FacetGroup fg = facetGroupWithShardIndex.facetGroup();
				int shardIndex = facetGroupWithShardIndex.shardIndex();

				for (FacetCount fc : fg.getFacetCountList()) {
					String facet = fc.getFacet();
					FixedBitSet shardSet = shardsReturned.computeIfAbsent(facet, k -> new FixedBitSet(shardReponses));
					long count = fc.getCount();
					facetCounts.addValue(facet, count);
					shardSet.set(shardIndex);
					minForShard[shardIndex] = count;
				}

				int shardFacets = countRequest.getShardFacets();
				int facetCountCount = fg.getFacetCountCount();
				if (facetCountCount < shardFacets || (shardFacets == -1)) {
					fullResults.set(shardIndex);
					minForShard[shardIndex] = 0;
				}
			}

			FacetGroup.Builder fg = FacetGroup.newBuilder();
			fg.setCountRequest(countRequest);

			int numberOfShards = shardIndexes.length;
			long maxValuePossibleMissing = 0;
			for (int i = 0; i < numberOfShards; i++) {
				maxValuePossibleMissing += minForShard[i];
			}

			boolean computeError = countRequest.getMaxFacets() > 0 && countRequest.getShardFacets() > 0 && numberOfShards > 1;
			boolean computePossibleMissing = computeError && (maxValuePossibleMissing != 0);

			SortedSet<FacetCountResult> sortedFacetResults = facetCounts.entrySet().stream()
					.map(entry -> new FacetCountResult(entry.getKey(), entry.getValue())).collect(Collectors.toCollection(TreeSet::new));

			int maxCount = countRequest.getMaxFacets();

			long minCountReturned = 0;

			int count = 0;
			for (FacetCountResult facet : sortedFacetResults) {

				FixedBitSet shardCount = shardsReturned.get(facet.getFacet());
				shardCount.or(fullResults);

				FacetCount.Builder facetCountBuilder = FacetCount.newBuilder().setFacet(facet.getFacet()).setCount(facet.getCount());

				long maxWithError = 0;
				if (computeError) {
					long maxError = 0;
					if (shardCount.cardinality() < numberOfShards) {
						for (int i = 0; i < numberOfShards; i++) {
							if (!shardCount.get(i)) {
								maxError += minForShard[i];
							}
						}
					}
					facetCountBuilder.setMaxError(maxError);
					maxWithError = maxError + facet.getCount();
				}

				count++;

				if (maxCount > 0 && count > maxCount) {

					if (computePossibleMissing) {
						if (maxWithError > maxValuePossibleMissing) {
							maxValuePossibleMissing = maxWithError;
						}
					}
					else {
						break;
					}
				}
				else {
					fg.addFacetCount(facetCountBuilder);
					minCountReturned = facet.getCount();
				}
			}

			if (!sortedFacetResults.isEmpty()) {
				if (maxValuePossibleMissing > minCountReturned) {
					fg.setPossibleMissing(true);
					fg.setMaxValuePossibleMissing(maxValuePossibleMissing);
				}
			}

			return fg.build();
		}
	}
}
