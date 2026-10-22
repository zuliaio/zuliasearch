package io.zulia.server.search.aggregation.stats;

import io.zulia.message.ZuliaQuery.FacetStats;
import io.zulia.message.ZuliaQuery.FacetStatsInternal;
import io.zulia.message.ZuliaQuery.SortValue;
import io.zulia.message.ZuliaQuery.StatGroup;
import io.zulia.message.ZuliaQuery.StatGroupInternal;
import io.zulia.message.ZuliaQuery.Percentile;
import io.zulia.message.ZuliaQuery.StatRequest;
import io.zulia.util.sketch.QuantileSketch;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;

public class StatCombinerTest {

	@Test
	public void testErrorBoundCalculation() {
		// Setup: 3 shards, each returning top 2 facets (shardFacets=2)
		// Shard 0: facetA (sum=100), facetB (sum=50)  -> min sum = 50
		// Shard 1: facetA (sum=80), facetC (sum=30)   -> min sum = 30
		// Shard 2: facetB (sum=60), facetC (sum=40)   -> min sum = 40
		//
		// facetA: present in shards 0,1; missing from shard 2 -> error bound = 40 (min sum of shard 2)
		// facetB: present in shards 0,2; missing from shard 1 -> error bound = 30 (min sum of shard 1)
		// facetC: present in shards 1,2; missing from shard 0 -> error bound = 50 (min sum of shard 0)

		StatRequest statRequest = StatRequest.newBuilder()
				.setNumericField("amount")
				.setMaxFacets(10)
				.setShardFacets(2)  // Not -1, so error bounds will be calculated
				.build();

		int shardCount = 3;
		StatCombiner combiner = new StatCombiner(statRequest, shardCount);

		// Shard 0: facetA=100, facetB=50 (sorted descending by sum)
		StatGroupInternal shard0 = StatGroupInternal.newBuilder()
				.setGlobalStats(buildFacetStats("", 150))
				.addFacetStats(buildFacetStats("facetA", 100))
				.addFacetStats(buildFacetStats("facetB", 50))
				.build();
		combiner.handleStatGroupForShard(shard0, 0);

		// Shard 1: facetA=80, facetC=30 (sorted descending by sum)
		StatGroupInternal shard1 = StatGroupInternal.newBuilder()
				.setGlobalStats(buildFacetStats("", 110))
				.addFacetStats(buildFacetStats("facetA", 80))
				.addFacetStats(buildFacetStats("facetC", 30))
				.build();
		combiner.handleStatGroupForShard(shard1, 1);

		// Shard 2: facetB=60, facetC=40 (sorted descending by sum)
		StatGroupInternal shard2 = StatGroupInternal.newBuilder()
				.setGlobalStats(buildFacetStats("", 100))
				.addFacetStats(buildFacetStats("facetB", 60))
				.addFacetStats(buildFacetStats("facetC", 40))
				.build();
		combiner.handleStatGroupForShard(shard2, 2);

		// Execute
		StatGroup result = combiner.getCombinedStatGroupAndConvertToExternalType();

		// Verify
		List<FacetStats> facetStatsList = result.getFacetStatsList();
		Assertions.assertEquals(3, facetStatsList.size());

		// Results are sorted by sum descending: facetA (180), facetB (110), facetC (70)
		FacetStats facetA = findFacetByName(facetStatsList, "facetA");
		FacetStats facetB = findFacetByName(facetStatsList, "facetB");
		FacetStats facetC = findFacetByName(facetStatsList, "facetC");

		Assertions.assertNotNull(facetA);
		Assertions.assertNotNull(facetB);
		Assertions.assertNotNull(facetC);

		// Verify sums are combined correctly
		Assertions.assertEquals(180, facetA.getSum().getLongValue()); // 100 + 80
		Assertions.assertEquals(110, facetB.getSum().getLongValue()); // 50 + 60
		Assertions.assertEquals(70, facetC.getSum().getLongValue());  // 30 + 40

		// Verify hasError flags
		Assertions.assertTrue(facetA.getHasError()); // Missing from shard 2
		Assertions.assertTrue(facetB.getHasError()); // Missing from shard 1
		Assertions.assertTrue(facetC.getHasError()); // Missing from shard 0

		// Verify error bounds
		// facetA missing from shard 2, whose min sum is 40
		Assertions.assertEquals(40, facetA.getMaxSumError().getLongValue());
		// facetB missing from shard 1, whose min sum is 30
		Assertions.assertEquals(30, facetB.getMaxSumError().getLongValue());
		// facetC missing from shard 0, whose min sum is 50
		Assertions.assertEquals(50, facetC.getMaxSumError().getLongValue());
	}

	@Test
	public void testErrorBoundWithMultipleMissingShards() {
		// Setup: 3 shards, facet only present in 1 shard
		// Shard 0: facetA (sum=100), facetB (sum=50)  -> min sum = 50
		// Shard 1: facetB (sum=80), facetC (sum=30)   -> min sum = 30
		// Shard 2: facetB (sum=60), facetC (sum=40)   -> min sum = 40
		//
		// facetA: present only in shard 0; missing from shards 1,2 -> error bound = 30 + 40 = 70

		StatRequest statRequest = StatRequest.newBuilder()
				.setNumericField("amount")
				.setMaxFacets(10)
				.setShardFacets(2)
				.build();

		int shardCount = 3;
		StatCombiner combiner = new StatCombiner(statRequest, shardCount);

		// Shard 0: facetA=100, facetB=50
		StatGroupInternal shard0 = StatGroupInternal.newBuilder()
				.setGlobalStats(buildFacetStats("", 150))
				.addFacetStats(buildFacetStats("facetA", 100))
				.addFacetStats(buildFacetStats("facetB", 50))
				.build();
		combiner.handleStatGroupForShard(shard0, 0);

		// Shard 1: facetB=80, facetC=30
		StatGroupInternal shard1 = StatGroupInternal.newBuilder()
				.setGlobalStats(buildFacetStats("", 110))
				.addFacetStats(buildFacetStats("facetB", 80))
				.addFacetStats(buildFacetStats("facetC", 30))
				.build();
		combiner.handleStatGroupForShard(shard1, 1);

		// Shard 2: facetB=60, facetC=40
		StatGroupInternal shard2 = StatGroupInternal.newBuilder()
				.setGlobalStats(buildFacetStats("", 100))
				.addFacetStats(buildFacetStats("facetB", 60))
				.addFacetStats(buildFacetStats("facetC", 40))
				.build();
		combiner.handleStatGroupForShard(shard2, 2);

		// Execute
		StatGroup result = combiner.getCombinedStatGroupAndConvertToExternalType();

		// Verify
		List<FacetStats> facetStatsList = result.getFacetStatsList();
		FacetStats facetA = findFacetByName(facetStatsList, "facetA");

		Assertions.assertNotNull(facetA);
		Assertions.assertTrue(facetA.getHasError());
		// Error bound = min sum of shard 1 (30) + min sum of shard 2 (40) = 70
		Assertions.assertEquals(70, facetA.getMaxSumError().getLongValue());
	}

	@Test
	public void testNoErrorWhenAllFacetsRequested() {
		// When shardFacets = -1, no error bounds should be calculated

		StatRequest statRequest = StatRequest.newBuilder()
				.setNumericField("amount")
				.setMaxFacets(10)
				.setShardFacets(-1)  // All facets requested
				.build();

		int shardCount = 2;
		StatCombiner combiner = new StatCombiner(statRequest, shardCount);

		// Shard 0: only has facetA
		StatGroupInternal shard0 = StatGroupInternal.newBuilder()
				.setGlobalStats(buildFacetStats("", 100))
				.addFacetStats(buildFacetStats("facetA", 100))
				.build();
		combiner.handleStatGroupForShard(shard0, 0);

		// Shard 1: only has facetB (different facet)
		StatGroupInternal shard1 = StatGroupInternal.newBuilder()
				.setGlobalStats(buildFacetStats("", 50))
				.addFacetStats(buildFacetStats("facetB", 50))
				.build();
		combiner.handleStatGroupForShard(shard1, 1);

		// Execute
		StatGroup result = combiner.getCombinedStatGroupAndConvertToExternalType();

		// Verify - no error flags when shardFacets = -1
		List<FacetStats> facetStatsList = result.getFacetStatsList();
		for (FacetStats fs : facetStatsList) {
			Assertions.assertFalse(fs.getHasError(), "Should not have error when shardFacets=-1");
		}
	}

	@Test
	public void testNoErrorWhenFacetPresentInAllShards() {
		// When a facet is present in all shards, no error for that facet

		StatRequest statRequest = StatRequest.newBuilder()
				.setNumericField("amount")
				.setMaxFacets(10)
				.setShardFacets(2)
				.build();

		int shardCount = 2;
		StatCombiner combiner = new StatCombiner(statRequest, shardCount);

		// Both shards have facetA
		StatGroupInternal shard0 = StatGroupInternal.newBuilder()
				.setGlobalStats(buildFacetStats("", 100))
				.addFacetStats(buildFacetStats("facetA", 100))
				.build();
		combiner.handleStatGroupForShard(shard0, 0);

		StatGroupInternal shard1 = StatGroupInternal.newBuilder()
				.setGlobalStats(buildFacetStats("", 50))
				.addFacetStats(buildFacetStats("facetA", 50))
				.build();
		combiner.handleStatGroupForShard(shard1, 1);

		// Execute
		StatGroup result = combiner.getCombinedStatGroupAndConvertToExternalType();

		// Verify - facetA present in all shards, no error
		FacetStats facetA = findFacetByName(result.getFacetStatsList(), "facetA");
		Assertions.assertNotNull(facetA);
		Assertions.assertFalse(facetA.getHasError());
		Assertions.assertEquals(150, facetA.getSum().getLongValue());
	}

	private static final double PRECISION = 0.001;

	@Test
	public void percentilesMatchOneSketchOverEveryShardValue() {
		// points out of order and repeated, the response keeps the requested order
		List<Double> points = List.of(0.5, 0.05, 0.99, 0.5, 0.0, 1.0, 0.25);
		StatRequest statRequest = StatRequest.newBuilder().setNumericField("amount").setMaxFacets(10).setShardFacets(-1).setPrecision(PRECISION)
				.addAllPercentiles(points).build();

		Random random = new Random(7);
		int shardCount = 4;
		StatCombiner combiner = new StatCombiner(statRequest, shardCount);
		QuantileSketch everyValue = new QuantileSketch(PRECISION, QuantileSketch.Mapping.CUBIC);
		for (int shard = 0; shard < shardCount; shard++) {
			DoubleDoubleStats shardStats = new DoubleDoubleStats(PRECISION);
			int values = 50 + random.nextInt(500);
			double scale = Math.pow(10, random.nextInt(6) - 2);
			for (int i = 0; i < values; i++) {
				double value = random.nextInt(40) == 0 ? 0 : (random.nextDouble() - 0.3) * scale;
				shardStats.newDoc(true);
				shardStats.newValue(value);
				everyValue.accept(value);
			}
			FacetStatsInternal facetStats = shardStats.buildResponse().setFacet("facetA").build();
			combiner.handleStatGroupForShard(StatGroupInternal.newBuilder().setGlobalStats(facetStats).addFacetStats(facetStats).build(), shard);
		}

		StatGroup result = combiner.getCombinedStatGroupAndConvertToExternalType();
		double[] expected = everyValue.quantiles(new double[] { 0.0, 0.05, 0.25, 0.5, 0.99, 1.0 });
		double[] expectedInRequestOrder = { expected[3], expected[1], expected[4], expected[3], expected[0], expected[5], expected[2] };
		assertPercentiles(points, expectedInRequestOrder, result.getGlobalStats().getPercentilesList());
		assertPercentiles(points, expectedInRequestOrder, findFacetByName(result.getFacetStatsList(), "facetA").getPercentilesList());
	}

	@Test
	public void shardsWithoutASketchAreSkippedAndEmptySketchesGiveNoPercentiles() {
		StatRequest statRequest = StatRequest.newBuilder().setNumericField("amount").setMaxFacets(10).setShardFacets(-1).setPrecision(PRECISION)
				.addPercentiles(0.5).build();
		StatCombiner combiner = new StatCombiner(statRequest, 3);

		DoubleDoubleStats withValues = new DoubleDoubleStats(PRECISION);
		for (double value : new double[] { 1, 2, 3, 4, 5 }) {
			withValues.newDoc(true);
			withValues.newValue(value);
		}
		DoubleDoubleStats noValues = new DoubleDoubleStats(PRECISION);
		noValues.newDoc(false);

		// shard 0 matched docs, shard 1 matched docs with no values, shard 2 matched nothing and sends the default message
		combiner.handleStatGroupForShard(StatGroupInternal.newBuilder().setGlobalStats(withValues.buildResponse().build())
				.addFacetStats(withValues.buildResponse().setFacet("facetA").build()).addFacetStats(noValues.buildResponse().setFacet("facetB").build())
				.build(), 0);
		combiner.handleStatGroupForShard(StatGroupInternal.newBuilder().setGlobalStats(noValues.buildResponse().build())
				.addFacetStats(noValues.buildResponse().setFacet("facetB").build()).build(), 1);
		combiner.handleStatGroupForShard(StatGroupInternal.newBuilder().setGlobalStats(FacetStatsInternal.getDefaultInstance()).build(), 2);

		StatGroup result = combiner.getCombinedStatGroupAndConvertToExternalType();
		QuantileSketch everyValue = new QuantileSketch(PRECISION, QuantileSketch.Mapping.CUBIC);
		for (double value : new double[] { 1, 2, 3, 4, 5 }) {
			everyValue.accept(value);
		}
		double median = everyValue.quantile(0.5);
		assertPercentiles(List.of(0.5), new double[] { median }, result.getGlobalStats().getPercentilesList());
		assertPercentiles(List.of(0.5), new double[] { median }, findFacetByName(result.getFacetStatsList(), "facetA").getPercentilesList());
		Assertions.assertEquals(0, findFacetByName(result.getFacetStatsList(), "facetB").getPercentilesCount());
		// five docs with values from shard 0, one without a value from shard 1, nothing from shard 2
		Assertions.assertEquals(6, result.getGlobalStats().getAllDocCount());
		Assertions.assertEquals(5, result.getGlobalStats().getDocCount());
	}

	private static void assertPercentiles(List<Double> points, double[] expected, List<Percentile> actual) {
		Assertions.assertEquals(points.size(), actual.size());
		for (int i = 0; i < points.size(); i++) {
			Assertions.assertEquals(points.get(i), actual.get(i).getPoint(), "point " + i);
			Assertions.assertEquals(expected[i], actual.get(i).getValue(), "value at " + points.get(i));
		}
	}

	private FacetStatsInternal buildFacetStats(String facet, long sum) {
		return FacetStatsInternal.newBuilder()
				.setFacet(facet)
				.setSum(SortValue.newBuilder().setLongValue(sum).setExists(true).build())
				.setMin(SortValue.newBuilder().setLongValue(sum).setExists(true).build())
				.setMax(SortValue.newBuilder().setLongValue(sum).setExists(true).build())
				.setDocCount(1)
				.setAllDocCount(1)
				.setValueCount(1)
				.build();
	}

	private FacetStats findFacetByName(List<FacetStats> facetStatsList, String name) {
		return facetStatsList.stream()
				.filter(fs -> fs.getFacet().equals(name))
				.findFirst()
				.orElse(null);
	}
}
