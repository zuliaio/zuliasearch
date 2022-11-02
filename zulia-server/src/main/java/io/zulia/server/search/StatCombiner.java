package io.zulia.server.search;

import com.datadoghq.sketch.ddsketch.DDSketch;
import com.datadoghq.sketch.ddsketch.DDSketchProtoBinding;
import com.datadoghq.sketch.ddsketch.DDSketches;
import com.datadoghq.sketch.ddsketch.store.UnboundedSizeDenseStore;
import io.zulia.message.ZuliaQuery;
import io.zulia.message.ZuliaQuery.StatGroupInternal;
import io.zulia.message.ZuliaQuery.StatRequest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class StatCombiner {

	public static class StatGroupWithShardIndex {
		private final StatGroupInternal statGroup;
		private final int shardIndex;

		public StatGroupWithShardIndex(StatGroupInternal statGroup, int shardIndex) {
			this.statGroup = statGroup;
			this.shardIndex = shardIndex;
		}

		public StatGroupInternal getStatGroup() {
			return statGroup;
		}

		public int getShardIndex() {
			return shardIndex;
		}
	}

	private final List<StatGroupWithShardIndex> statGroups;
	private final int[] shardIndexes;
	private final StatRequest statRequest;
	private final int shardReponses;

	public StatCombiner(StatRequest statRequest, int shardReponses) {
		this.statRequest = statRequest;
		this.shardReponses = shardReponses;
		this.statGroups = new ArrayList<>(shardReponses);
		this.shardIndexes = new int[shardReponses];
	}

	public void handleStatGroupForShard(StatGroupInternal statGroup, int shardIndex) {
		statGroups.add(new StatGroupWithShardIndex(statGroup, shardIndex));
	}

	public ZuliaQuery.StatGroup getCombinedStatGroupAndConvertToExternalType() {
		//TODO(Ian): Multiple indexes means what?
		List<StatGroupInternal> internalGroups = statGroups.stream().map(StatGroupWithShardIndex::getStatGroup).toList();
		List<ZuliaQuery.FacetStatsInternal> globalStatsInternal = internalGroups.stream().map(ZuliaQuery.StatGroupInternal::getGlobalStats).toList();

		// TODO(Ian): Error checking! What if different global stats have different facet names?

		// Create a map grouping of lists of facets to be merged
		HashMap<String, List<ZuliaQuery.FacetStatsInternal>> facetStatsGroups = new HashMap<>();
		internalGroups.forEach(statGroupInternal ->
				// Group all sets of facet stats
				statGroupInternal.getFacetStatsList().forEach(facetStatsInternal -> {
					String localName = facetStatsInternal.getFacet();
					List<ZuliaQuery.FacetStatsInternal> temp2 = facetStatsGroups.getOrDefault(localName, new ArrayList<>());
					temp2.add(facetStatsInternal);
					facetStatsGroups.put(localName, temp2);
				}));

		// Send each group through the converter to generate a list
		ZuliaQuery.FacetStats globalStats = convertAndCombineFacetStats(globalStatsInternal);
		List<ZuliaQuery.FacetStats> facetStats = facetStatsGroups.values().stream().map(this::convertAndCombineFacetStats).toList();

		// Generate return type
		return ZuliaQuery.StatGroup.newBuilder().setStatRequest(this.statRequest).setGlobalStats(globalStats).addAllFacetStats(facetStats).build();
	}

	/**
	 * Combines a list of FacetStats Internal type into a single "merged" FacetStats type which can be returned to the calling user. The will combine DDSketches
	 * into a final single sketch  and then retrieves the requested percentiles.
	 *
	 * @param internalStats List of internal stat messages to parse and combine into a return type
	 * @return Combined FacetStats message object ready to be returned to the user
	 */
	private ZuliaQuery.FacetStats convertAndCombineFacetStats(List<ZuliaQuery.FacetStatsInternal> internalStats) {
		String facetName = internalStats.get(0).getFacet();

		// Populate percentiles if they were requested correctly
		List<ZuliaQuery.Percentile> percentiles = new ArrayList<>();
		if (statRequest.getPrecision() > 0.0 && !statRequest.getPercentilesList().isEmpty()) {
			// Build initial sketch and merge other sketches into this one
			DDSketch combinedSketch = DDSketches.unboundedDense(statRequest.getPrecision());
			for (ZuliaQuery.FacetStatsInternal fsi : internalStats) {
				// Handle DDSketches
				DDSketch sketch = DDSketchProtoBinding.fromProto(UnboundedSizeDenseStore::new, fsi.getStatSketch());
				combinedSketch.mergeWith(sketch);
			}

			// Get all percentiles
			if (!combinedSketch.isEmpty()) {
				for (Double point : statRequest.getPercentilesList()) {
					if (point >= 0.0 && point <= 100.0) {
						percentiles.add(ZuliaQuery.Percentile.newBuilder().setPoint(point).setValue(combinedSketch.getValueAtQuantile(point)).build());
					}
				}
			}
		}

		// Accumulate the local stats
		StatCarrier carrier = new StatCarrier();
		internalStats.forEach(carrier::addStat);

		// Build combined final FacetStats that can be returned to the user
		return ZuliaQuery.FacetStats.newBuilder().setFacet(facetName).setMin(carrier.getMin()).setMax(carrier.getMax()).setSum(carrier.getSum())
				.setDocCount(carrier.docCount).setAllDocCount(carrier.allDocCount).setValueCount(carrier.valueCount).addAllPercentiles(percentiles).build();
	}

	private static class StatCarrier {
		private long docCount = 0;
		private long allDocCount = 0;
		private long valueCount = 0;

		private double doubleSum = 0;
		private double doubleMinValue = Double.POSITIVE_INFINITY;
		private double doubleMaxValue = Double.NEGATIVE_INFINITY;

		private long longSum = 0;
		private long longMinValue = Long.MAX_VALUE;
		private long longMaxValue = Long.MIN_VALUE;

		public StatCarrier() { }

		/**
		 * Handles combining a new stat value into the existing stat value set
		 *
		 * @param fsi Facet stats to merge into this dataset
		 */
		private void addStat(ZuliaQuery.FacetStatsInternal fsi) {
			// Handle max/min values
			doubleMaxValue = fsi.getMax().getDoubleValue() > doubleMaxValue ? fsi.getMax().getDoubleValue() : doubleMaxValue;
			longMaxValue = fsi.getMax().getLongValue() > longMaxValue ? fsi.getMax().getLongValue() : longMaxValue;
			doubleMinValue = fsi.getMin().getDoubleValue() < doubleMinValue ? fsi.getMin().getDoubleValue() : doubleMinValue;
			longMinValue = fsi.getMin().getLongValue() < longMinValue ? fsi.getMin().getLongValue() : longMinValue;

			// Handle counts and sums
			doubleSum += fsi.getSum().getDoubleValue();
			longSum += fsi.getSum().getLongValue();
			docCount += fsi.getDocCount();
			allDocCount += fsi.getAllDocCount();
			valueCount += fsi.getValueCount();
		}

		private ZuliaQuery.SortValue getMin() {
			return ZuliaQuery.SortValue.newBuilder().setDoubleValue(doubleMinValue).setLongValue(longMinValue).build();
		}

		private ZuliaQuery.SortValue getMax() {
			return ZuliaQuery.SortValue.newBuilder().setDoubleValue(doubleMaxValue).setLongValue(longMaxValue).build();
		}

		private ZuliaQuery.SortValue getSum() {
			return ZuliaQuery.SortValue.newBuilder().setDoubleValue(doubleSum).setLongValue(longSum).build();
		}
	}
}
