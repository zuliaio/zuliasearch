package io.zulia.server.search.stat;

import com.datadoghq.sketch.ddsketch.DDSketch;
import com.datadoghq.sketch.ddsketch.DDSketchProtoBinding;
import com.datadoghq.sketch.ddsketch.DDSketches;
import com.datadoghq.sketch.ddsketch.store.UnboundedSizeDenseStore;
import io.zulia.message.ZuliaQuery;
import io.zulia.message.ZuliaQuery.FacetStats;
import io.zulia.message.ZuliaQuery.FacetStatsInternal;
import io.zulia.message.ZuliaQuery.Percentile;
import io.zulia.message.ZuliaQuery.SortValue;
import io.zulia.message.ZuliaQuery.StatGroup;
import io.zulia.message.ZuliaQuery.StatGroupInternal;
import io.zulia.message.ZuliaQuery.StatRequest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class StatCombiner {

	public record StatGroupWithShardIndex(StatGroupInternal statGroup, int shardIndex) {

	}

	private record FacetStatsWithShardIndex(FacetStatsInternal facetStats, int shardIndex) {

	}

	private final List<StatGroupWithShardIndex> statGroups;
	private final StatRequest statRequest;
	private final int shardReponses;

	public StatCombiner(StatRequest statRequest, int shardReponses) {
		this.statRequest = statRequest;
		this.shardReponses = shardReponses;
		this.statGroups = new ArrayList<>(shardReponses);
	}

	public void handleStatGroupForShard(StatGroupInternal statGroup, int shardIndex) {
		statGroups.add(new StatGroupWithShardIndex(statGroup, shardIndex));
	}

	public ZuliaQuery.StatGroup getCombinedStatGroupAndConvertToExternalType() {
		// Get global stats
		List<FacetStatsWithShardIndex> globalStatsInternal = new ArrayList<>();
		statGroups.forEach(sg -> globalStatsInternal.add(new FacetStatsWithShardIndex(sg.statGroup().getGlobalStats(), sg.shardIndex())));

		// Create a map grouping of lists of facets to be merged
		HashMap<String, List<FacetStatsWithShardIndex>> facetStatsGroups = new HashMap<>();
		statGroups.forEach(statGroup ->
				// Group all sets of facet stats
				statGroup.statGroup().getFacetStatsList().forEach(facetStats -> {
					String localName = facetStats.getFacet();
					List<FacetStatsWithShardIndex> temp2 = facetStatsGroups.getOrDefault(localName, new ArrayList<>());
					temp2.add(new FacetStatsWithShardIndex(facetStats, statGroup.shardIndex()));
					facetStatsGroups.put(localName, temp2);
				}));

		// Send each group through the converter to generate a list
		FacetStats.Builder globalStats = convertAndCombineFacetStats(globalStatsInternal);
		List<FacetStats.Builder> facetStats = facetStatsGroups.values().stream().map(this::convertAndCombineFacetStats).toList();

		// Find / bound errors in the sum element
		for (FacetStats.Builder fs : facetStats) {
			if (fs.getHasError()) {
				// The error is the summation across all shards of the smallest value returned by each shard which did not return this facet
				List<Integer> missing = getNonReportingShards(facetStatsGroups.get(fs.getFacet()));
				SortValue errorBound = getErrorBound(missing);
				fs.setMaxSumError(errorBound);
			}
		}

		// Generate return message
		return StatGroup.newBuilder().setStatRequest(this.statRequest).setGlobalStats(globalStats)
				.addAllFacetStats(facetStats.stream().map(FacetStats.Builder::build).sorted(this::reverseCompareFacetStats).toList()).build();
	}

	/**
	 * Comparator method for complex container type Facet Stats. Sorts by sum, if it exists and largest to smallest, rather than natural order
	 *
	 * @param o1 Any facet stat value
	 * @param o2 Any other facet stat value
	 * @return Result of [Type].compare for actively populated datatype
	 */
	private int reverseCompareFacetStats(FacetStats o1, FacetStats o2) {
		if (o1.hasSum() && o2.hasSum()) {
			// Reversing the arguments reverses the comparison by the comparators
			SortValue sv1 = o2.getSum();
			SortValue sv2 = o1.getSum();
			// Compare each type until one is not equal. If all are equal, values are equal
			int comp = Double.compare(sv1.getDoubleValue(), sv2.getDoubleValue());
			if (comp == 0)
				comp = Integer.compare(sv1.getIntegerValue(), sv2.getIntegerValue());
			if (comp == 0)
				comp = Long.compare(sv1.getLongValue(), sv2.getLongValue());
			if (comp == 0)
				comp = Float.compare(sv1.getFloatValue(), sv2.getFloatValue());
			if (comp == 0)
				comp = Long.compare(sv1.getDateValue(), sv2.getDateValue());
			return comp;
		}
		else {
			return 0;
		}
	}

	/**
	 * Determines which shards are not represented in a list of facet stats
	 *
	 * @param facetStats list to interrogate
	 * @return List of missing shard indexes
	 */
	private List<Integer> getNonReportingShards(List<FacetStatsWithShardIndex> facetStats) {
		boolean[] mask = new boolean[shardReponses]; // Defaults to false (arrays are also fast)
		facetStats.forEach(f -> mask[f.shardIndex()] = true); // Set values to true

		List<Integer> missingShards = new ArrayList<>();
		for (int i = 0; i < shardReponses; i++) {
			if (!mask[i]) {
				missingShards.add(i);
			}
		}

		return missingShards;
	}

	/**
	 * Gets the error bound by finding the sum of the minimums across all non-reporting shards
	 *
	 * @param missingIndexes list of shard indexes to evaluate
	 * @return SortValue representing the error bound
	 */
	private SortValue getErrorBound(List<Integer> missingIndexes) {
		List<StatCarrier> statCarriers = new ArrayList<>();
		for (StatGroupWithShardIndex sgi : statGroups) {
			if (missingIndexes.contains(sgi.shardIndex)) {
				StatCarrier sc = new StatCarrier();
				sgi.statGroup().getFacetStatsList().forEach(facetStatsInternal -> sc.addErrorStat(facetStatsInternal.getSum()));
				statCarriers.add(sc);
			}
		}

		// Sum the error values found earlier
		SortValue.Builder errorBound = SortValue.newBuilder();
		for (StatCarrier sc : statCarriers) {
			errorBound = StatCarrier.addToSum(errorBound, sc.getErrorValue());
		}

		return errorBound.build();
	}

	/**
	 * Combines a list of FacetStats Internal type into a single "merged" FacetStats type which can be returned to the calling user. The will combine DDSketches
	 * into a final single sketch  and then retrieves the requested percentiles.
	 *
	 * @param internalStats List of internal stat messages to parse and combine into a return type
	 * @return Combined FacetStats message object ready to be returned to the user
	 */
	private FacetStats.Builder convertAndCombineFacetStats(List<FacetStatsWithShardIndex> internalStats) {
		String facetName = internalStats.get(0).facetStats().getFacet();

		// If there are missing shard responses AND not all facets were requested there must be error
		// -1 means all facets were requested so no error is possible in this case
		boolean hasError = statRequest.getShardFacets() != -1 && internalStats.size() < shardReponses;

		// No results to convert here. Return a blank value
		if (internalStats.get(0).facetStats().getSerializedSize() == 0) {
			return FacetStats.newBuilder();
		}

		// Populate percentiles if they were requested correctly
		List<Percentile> percentiles = new ArrayList<>();
		if (statRequest.getPrecision() > 0.0 && !statRequest.getPercentilesList().isEmpty()) {
			// Build initial sketch and merge other sketches into this one
			DDSketch combinedSketch = DDSketches.unboundedDense(statRequest.getPrecision());
			for (FacetStatsWithShardIndex fsi : internalStats) {
				// Handle DDSketches
				DDSketch sketch = DDSketchProtoBinding.fromProto(UnboundedSizeDenseStore::new, fsi.facetStats().getStatSketch());
				combinedSketch.mergeWith(sketch);
			}

			// Get all percentiles
			if (!combinedSketch.isEmpty()) {
				for (Double point : statRequest.getPercentilesList()) {
					if (point >= 0.0 && point <= 100.0) {
						percentiles.add(Percentile.newBuilder().setPoint(point).setValue(combinedSketch.getValueAtQuantile(point)).build());
					}
				}
			}
		}

		// Accumulate the local stats
		StatCarrier carrier = new StatCarrier();
		internalStats.stream().map(FacetStatsWithShardIndex::facetStats).toList().forEach(carrier::addStat);

		// Build combined final FacetStats that can be returned to the user
		return FacetStats.newBuilder().setFacet(facetName).setMin(carrier.getMin()).setMax(carrier.getMax()).setSum(carrier.getSum())
				.setDocCount(carrier.docCount).setAllDocCount(carrier.allDocCount).setValueCount(carrier.valueCount).addAllPercentiles(percentiles)
				.setHasError(hasError);
	}

	private static class StatCarrier {
		private long docCount = 0;
		private long allDocCount = 0;
		private long valueCount = 0;

		private SortValue.Builder sum = null;
		private SortValue.Builder maxValue = null;
		private SortValue.Builder minValue = null;
		private SortValue.Builder errorValue = null; // Will be a min

		public StatCarrier() {
		}

		/**
		 * Handles combining a new stat value into the existing stat value set
		 *
		 * @param fsi Facet stats to merge into this dataset
		 */
		private void addStat(FacetStatsInternal fsi) {

			// Update actual variables
			maxValue = updateMaxValue(maxValue, fsi.getMax());
			minValue = updateMinValue(minValue, fsi.getMin());
			sum = addToSum(sum, fsi.getSum());

			// Handle counts and sums
			docCount += fsi.getDocCount();
			allDocCount += fsi.getAllDocCount();
			valueCount += fsi.getValueCount();
		}

		/**
		 * Utilize this mechanism for tracking min of an error stat
		 *
		 * @param errorMetric error metric to minimize
		 */
		private void addErrorStat(SortValue errorMetric) {
			errorValue = updateMinValue(errorValue, errorMetric);
		}

		private SortValue getMin() {
			return minValue.build();
		}

		private SortValue getMax() {
			return maxValue.build();
		}

		private SortValue getSum() {
			return sum.build();
		}

		private SortValue getErrorValue() {
			return errorValue.build();
		}

		/**
		 * Method for merging new data into a SortValue builder for Max value (also handles error bounding for min value)
		 *
		 * @param builder builder to maximize
		 * @param value   new value to add to this builder
		 */
		private static SortValue.Builder updateMaxValue(SortValue.Builder builder, SortValue value) {
			// Initialization
			if (builder == null) {
				builder = value.toBuilder();
			}

			// Handle merge
			builder.setExists(builder.getExists() || value.getExists());

			if (value.getDoubleValue() > builder.getDoubleValue()) {
				builder.setDoubleValue(value.getDoubleValue());
			}
			if (value.getDateValue() > builder.getDateValue()) {
				builder.setDateValue(value.getDateValue());
			}
			if (value.getFloatValue() > builder.getFloatValue()) {
				builder.setFloatValue(value.getFloatValue());
			}
			if (value.getLongValue() > builder.getLongValue()) {
				builder.setLongValue(value.getLongValue());
			}
			if (value.getIntegerValue() > builder.getIntegerValue()) {
				builder.setIntegerValue(value.getIntegerValue());
			}
			if (value.getStringValue().compareTo(builder.getStringValue()) > 0) {
				builder.setStringValue(value.getStringValue());
			}

			return builder;
		}

		/**
		 * Method for merging new data into a SortValue builder for min values
		 *
		 * @param builder builder to minimize
		 * @param value   new value to add to this builder
		 */
		private static SortValue.Builder updateMinValue(SortValue.Builder builder, SortValue value) {
			// Initialization
			if (builder == null) {
				return value.toBuilder();
			}

			// Handle merge
			builder.setExists(builder.getExists() || value.getExists());

			if (value.getDoubleValue() < builder.getDoubleValue()) {
				builder.setDoubleValue(value.getDoubleValue());
			}
			if (value.getDateValue() < builder.getDateValue()) {
				builder.setDateValue(value.getDateValue());
			}
			if (value.getFloatValue() < builder.getFloatValue()) {
				builder.setFloatValue(value.getFloatValue());
			}
			if (value.getLongValue() < builder.getLongValue()) {
				builder.setLongValue(value.getLongValue());
			}
			if (value.getIntegerValue() < builder.getIntegerValue()) {
				builder.setIntegerValue(value.getIntegerValue());
			}
			if (value.getStringValue().compareTo(builder.getStringValue()) < 0) {
				builder.setStringValue(value.getStringValue());
			}

			return builder;
		}

		/**
		 * Method for merging new data into a SortValue builder for sort value
		 *
		 * @param builder for a sort value to update with new values
		 * @param value   new value to add to this builder
		 */
		public static SortValue.Builder addToSum(SortValue.Builder builder, SortValue value) {
			// Initialization
			if (builder == null) {
				return value.toBuilder();
			}

			// Handle merge
			builder.setExists(builder.getExists() || value.getExists());
			builder.setDoubleValue(builder.getDoubleValue() + value.getDoubleValue());
			builder.setDateValue(builder.getDateValue() + value.getDateValue());
			builder.setFloatValue(builder.getFloatValue() + value.getFloatValue());
			builder.setLongValue(builder.getLongValue() + value.getLongValue());
			builder.setIntegerValue(builder.getIntegerValue() + value.getIntegerValue());
			// TODO(Swepston): Sum of strings makes no sense
			return builder;
		}
	}
}
