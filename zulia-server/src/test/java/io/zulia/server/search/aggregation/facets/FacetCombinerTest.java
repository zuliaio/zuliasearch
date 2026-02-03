package io.zulia.server.search.aggregation.facets;

import io.zulia.message.ZuliaQuery.CountRequest;
import io.zulia.message.ZuliaQuery.FacetCount;
import io.zulia.message.ZuliaQuery.FacetGroup;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class FacetCombinerTest {

	@Test
	public void testErrorBoundCalculation() {
		// Setup: 3 shards, each returning top 2 facets (shardFacets=2)
		// Shard 0: facetA (count=100), facetB (count=50)  -> min count = 50
		// Shard 1: facetA (count=80), facetC (count=30)   -> min count = 30
		// Shard 2: facetB (count=60), facetC (count=40)   -> min count = 40
		//
		// facetA: present in shards 0,1; missing from shard 2 -> error = 40 (min count of shard 2)
		// facetB: present in shards 0,2; missing from shard 1 -> error = 30 (min count of shard 1)
		// facetC: present in shards 1,2; missing from shard 0 -> error = 50 (min count of shard 0)

		CountRequest countRequest = CountRequest.newBuilder()
				.setMaxFacets(10)
				.setShardFacets(2)
				.build();

		int shardCount = 3;
		FacetCombiner combiner = new FacetCombiner(countRequest, shardCount);

		// Shard 0: facetA=100, facetB=50 (sorted descending by count)
		FacetGroup shard0 = FacetGroup.newBuilder()
				.addFacetCount(FacetCount.newBuilder().setFacet("facetA").setCount(100))
				.addFacetCount(FacetCount.newBuilder().setFacet("facetB").setCount(50))
				.build();
		combiner.handleFacetGroupForShard(shard0, 0);

		// Shard 1: facetA=80, facetC=30 (sorted descending by count)
		FacetGroup shard1 = FacetGroup.newBuilder()
				.addFacetCount(FacetCount.newBuilder().setFacet("facetA").setCount(80))
				.addFacetCount(FacetCount.newBuilder().setFacet("facetC").setCount(30))
				.build();
		combiner.handleFacetGroupForShard(shard1, 1);

		// Shard 2: facetB=60, facetC=40 (sorted descending by count)
		FacetGroup shard2 = FacetGroup.newBuilder()
				.addFacetCount(FacetCount.newBuilder().setFacet("facetB").setCount(60))
				.addFacetCount(FacetCount.newBuilder().setFacet("facetC").setCount(40))
				.build();
		combiner.handleFacetGroupForShard(shard2, 2);

		// Execute
		FacetGroup result = combiner.getCombinedFacetGroup();

		// Verify
		List<FacetCount> facetCounts = result.getFacetCountList();
		Assertions.assertEquals(3, facetCounts.size());

		// Results are sorted by count descending: facetA (180), facetB (110), facetC (70)
		FacetCount facetA = findFacetByName(facetCounts, "facetA");
		FacetCount facetB = findFacetByName(facetCounts, "facetB");
		FacetCount facetC = findFacetByName(facetCounts, "facetC");

		Assertions.assertNotNull(facetA);
		Assertions.assertNotNull(facetB);
		Assertions.assertNotNull(facetC);

		// Verify counts are combined correctly
		Assertions.assertEquals(180, facetA.getCount()); // 100 + 80
		Assertions.assertEquals(110, facetB.getCount()); // 50 + 60
		Assertions.assertEquals(70, facetC.getCount());  // 30 + 40

		// Verify error bounds
		// facetA missing from shard 2, whose min count is 40
		Assertions.assertEquals(40, facetA.getMaxError());
		// facetB missing from shard 1, whose min count is 30
		Assertions.assertEquals(30, facetB.getMaxError());
		// facetC missing from shard 0, whose min count is 50
		Assertions.assertEquals(50, facetC.getMaxError());
	}

	@Test
	public void testErrorBoundWithMultipleMissingShards() {
		// Setup: 3 shards, facet only present in 1 shard
		// Shard 0: facetA (count=100), facetB (count=50)  -> min count = 50
		// Shard 1: facetB (count=80), facetC (count=30)   -> min count = 30
		// Shard 2: facetB (count=60), facetC (count=40)   -> min count = 40
		//
		// facetA: present only in shard 0; missing from shards 1,2 -> error = 30 + 40 = 70

		CountRequest countRequest = CountRequest.newBuilder()
				.setMaxFacets(10)
				.setShardFacets(2)
				.build();

		int shardCount = 3;
		FacetCombiner combiner = new FacetCombiner(countRequest, shardCount);

		// Shard 0: facetA=100, facetB=50
		FacetGroup shard0 = FacetGroup.newBuilder()
				.addFacetCount(FacetCount.newBuilder().setFacet("facetA").setCount(100))
				.addFacetCount(FacetCount.newBuilder().setFacet("facetB").setCount(50))
				.build();
		combiner.handleFacetGroupForShard(shard0, 0);

		// Shard 1: facetB=80, facetC=30
		FacetGroup shard1 = FacetGroup.newBuilder()
				.addFacetCount(FacetCount.newBuilder().setFacet("facetB").setCount(80))
				.addFacetCount(FacetCount.newBuilder().setFacet("facetC").setCount(30))
				.build();
		combiner.handleFacetGroupForShard(shard1, 1);

		// Shard 2: facetB=60, facetC=40
		FacetGroup shard2 = FacetGroup.newBuilder()
				.addFacetCount(FacetCount.newBuilder().setFacet("facetB").setCount(60))
				.addFacetCount(FacetCount.newBuilder().setFacet("facetC").setCount(40))
				.build();
		combiner.handleFacetGroupForShard(shard2, 2);

		// Execute
		FacetGroup result = combiner.getCombinedFacetGroup();

		// Verify
		FacetCount facetA = findFacetByName(result.getFacetCountList(), "facetA");

		Assertions.assertNotNull(facetA);
		Assertions.assertEquals(100, facetA.getCount());
		// Error = min count of shard 1 (30) + min count of shard 2 (40) = 70
		Assertions.assertEquals(70, facetA.getMaxError());
	}

	@Test
	public void testNoErrorWhenAllFacetsRequested() {
		// When shardFacets = -1, no error bounds should be calculated

		CountRequest countRequest = CountRequest.newBuilder()
				.setMaxFacets(10)
				.setShardFacets(-1)  // All facets requested
				.build();

		int shardCount = 2;
		FacetCombiner combiner = new FacetCombiner(countRequest, shardCount);

		// Shard 0: only has facetA
		FacetGroup shard0 = FacetGroup.newBuilder()
				.addFacetCount(FacetCount.newBuilder().setFacet("facetA").setCount(100))
				.build();
		combiner.handleFacetGroupForShard(shard0, 0);

		// Shard 1: only has facetB (different facet)
		FacetGroup shard1 = FacetGroup.newBuilder()
				.addFacetCount(FacetCount.newBuilder().setFacet("facetB").setCount(50))
				.build();
		combiner.handleFacetGroupForShard(shard1, 1);

		// Execute
		FacetGroup result = combiner.getCombinedFacetGroup();

		// Verify - no error when shardFacets = -1 (minForShard is 0 for all shards)
		for (FacetCount fc : result.getFacetCountList()) {
			Assertions.assertEquals(0, fc.getMaxError(), "Should have no error when shardFacets=-1");
		}
	}

	@Test
	public void testNoErrorWhenFacetPresentInAllShards() {
		// When a facet is present in all shards, no error for that facet

		CountRequest countRequest = CountRequest.newBuilder()
				.setMaxFacets(10)
				.setShardFacets(2)
				.build();

		int shardCount = 2;
		FacetCombiner combiner = new FacetCombiner(countRequest, shardCount);

		// Both shards have facetA and facetB
		FacetGroup shard0 = FacetGroup.newBuilder()
				.addFacetCount(FacetCount.newBuilder().setFacet("facetA").setCount(100))
				.addFacetCount(FacetCount.newBuilder().setFacet("facetB").setCount(50))
				.build();
		combiner.handleFacetGroupForShard(shard0, 0);

		FacetGroup shard1 = FacetGroup.newBuilder()
				.addFacetCount(FacetCount.newBuilder().setFacet("facetA").setCount(80))
				.addFacetCount(FacetCount.newBuilder().setFacet("facetB").setCount(40))
				.build();
		combiner.handleFacetGroupForShard(shard1, 1);

		// Execute
		FacetGroup result = combiner.getCombinedFacetGroup();

		// Verify - facets present in all shards have no error
		FacetCount facetA = findFacetByName(result.getFacetCountList(), "facetA");
		FacetCount facetB = findFacetByName(result.getFacetCountList(), "facetB");

		Assertions.assertNotNull(facetA);
		Assertions.assertNotNull(facetB);
		Assertions.assertEquals(180, facetA.getCount());
		Assertions.assertEquals(90, facetB.getCount());
		Assertions.assertEquals(0, facetA.getMaxError());
		Assertions.assertEquals(0, facetB.getMaxError());
	}

	@Test
	public void testSingleShardNoError() {
		// Single shard should return the facet group as-is

		CountRequest countRequest = CountRequest.newBuilder()
				.setMaxFacets(10)
				.setShardFacets(2)
				.build();

		int shardCount = 1;
		FacetCombiner combiner = new FacetCombiner(countRequest, shardCount);

		FacetGroup shard0 = FacetGroup.newBuilder()
				.addFacetCount(FacetCount.newBuilder().setFacet("facetA").setCount(100))
				.addFacetCount(FacetCount.newBuilder().setFacet("facetB").setCount(50))
				.build();
		combiner.handleFacetGroupForShard(shard0, 0);

		// Execute
		FacetGroup result = combiner.getCombinedFacetGroup();

		// Verify - single shard returns as-is
		Assertions.assertEquals(2, result.getFacetCountCount());
		Assertions.assertEquals("facetA", result.getFacetCount(0).getFacet());
		Assertions.assertEquals(100, result.getFacetCount(0).getCount());
	}

	@Test
	public void testPossibleMissing() {
		// Test that possibleMissing is set when there could be facets not returned

		CountRequest countRequest = CountRequest.newBuilder()
				.setMaxFacets(2)  // Only return top 2
				.setShardFacets(2)
				.build();

		int shardCount = 2;
		FacetCombiner combiner = new FacetCombiner(countRequest, shardCount);

		// Shard 0: facetA=100, facetB=50
		FacetGroup shard0 = FacetGroup.newBuilder()
				.addFacetCount(FacetCount.newBuilder().setFacet("facetA").setCount(100))
				.addFacetCount(FacetCount.newBuilder().setFacet("facetB").setCount(50))
				.build();
		combiner.handleFacetGroupForShard(shard0, 0);

		// Shard 1: facetA=80, facetC=60
		FacetGroup shard1 = FacetGroup.newBuilder()
				.addFacetCount(FacetCount.newBuilder().setFacet("facetA").setCount(80))
				.addFacetCount(FacetCount.newBuilder().setFacet("facetC").setCount(60))
				.build();
		combiner.handleFacetGroupForShard(shard1, 1);

		// Execute
		FacetGroup result = combiner.getCombinedFacetGroup();

		// With maxFacets=2, we get facetA (180) and facetC (60) or facetB depending on sorting
		// facetB has count=50, facetC has count=60
		// Combined sorted: facetA=180, facetC=60, facetB=50
		// Top 2: facetA, facetC
		Assertions.assertEquals(2, result.getFacetCountCount());

		// possibleMissing should be true since facetB was cut off and could have higher count with error
		// facetB has count=50, missing from shard 1 (min=60), so maxWithError = 50+60 = 110
		// This is > minCountReturned (60 for facetC)
		Assertions.assertTrue(result.getPossibleMissing());
	}

	private FacetCount findFacetByName(List<FacetCount> facetCounts, String name) {
		return facetCounts.stream()
				.filter(fc -> fc.getFacet().equals(name))
				.findFirst()
				.orElse(null);
	}
}
