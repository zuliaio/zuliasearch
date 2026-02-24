package io.zulia.server.search.aggregation.facets;

import com.koloboke.collect.map.hash.HashObjLongMap;
import com.koloboke.collect.map.hash.HashObjLongMaps;
import io.zulia.message.ZuliaQuery;
import io.zulia.message.ZuliaQuery.FacetCount;
import io.zulia.message.ZuliaQuery.FacetGroup;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
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

	public boolean isComplete() {
		return facetGroups.size() == shardReponses;
	}

	public FacetGroup getCombinedFacetGroup() {
		if (facetGroups.size() == 1) {
			return facetGroups.getFirst().facetGroup();
		}
		else {

			HashObjLongMap<String> facetCounts = HashObjLongMaps.newMutableMap();
			Map<String, BitSet> shardsReturned = new HashMap<>();
			BitSet fullResults = new BitSet(shardReponses);
			long[] minForShard = new long[shardReponses];

			for (FacetGroupWithShardIndex facetGroupWithShardIndex : facetGroups) {
				FacetGroup fg = facetGroupWithShardIndex.facetGroup();
				int shardIndex = facetGroupWithShardIndex.shardIndex();

				List<FacetCount> facetCountList = fg.getFacetCountList();
				for (FacetCount fc : facetCountList) {
					String facet = fc.getFacet();
					BitSet shardSet = shardsReturned.computeIfAbsent(facet, k -> new BitSet(shardReponses));
					facetCounts.addValue(facet, fc.getCount());
					shardSet.set(shardIndex);
				}

				int shardFacets = countRequest.getShardFacets();
				int facetCountCount = facetCountList.size();
				if (facetCountCount < shardFacets || (shardFacets == -1)) {
					fullResults.set(shardIndex);
					minForShard[shardIndex] = 0;
				}
				else if (!facetCountList.isEmpty()) {
					// List is sorted descending by count, so last element has minimum count
					minForShard[shardIndex] = facetCountList.getLast().getCount();
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

				BitSet shardCount = shardsReturned.get(facet.getFacet());
				shardCount.or(fullResults);

				FacetCount.Builder facetCountBuilder = FacetCount.newBuilder().setFacet(facet.getFacet()).setCount(facet.getCount());

				long maxWithError = 0;
				if (computeError) {
					long maxError = 0;
					if (shardCount.cardinality() < numberOfShards) {
						// Iterate only through missing shards using nextClearBit
						for (int i = shardCount.nextClearBit(0); i < numberOfShards; i = shardCount.nextClearBit(i + 1)) {
							maxError += minForShard[i];
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
