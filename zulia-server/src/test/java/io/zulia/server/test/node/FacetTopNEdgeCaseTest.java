package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.CountFacet;
import io.zulia.client.command.builder.FilterQuery;
import io.zulia.client.command.builder.Search;
import io.zulia.client.command.builder.StatFacet;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.SearchResult;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.message.ZuliaQuery;
import io.zulia.message.ZuliaQuery.FacetCount;
import io.zulia.server.test.node.shared.NodeExtension;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * Tests facet topN behavior around the collect-and-sort vs priority queue boundary.
 * With 10 unique facet values, shouldCollectAndSort threshold is cardinality/2 = 5,
 * so topN=4 uses PQ and topN=5+ uses collect-and-sort.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class FacetTopNEdgeCaseTest {

	@RegisterExtension
	static final NodeExtension nodeExtension = new NodeExtension(1);

	private static final String SINGLE_SHARD_INDEX = "facetEdgeSingleShard";
	private static final String MULTI_SHARD_INDEX = "facetEdgeMultiShard";
	private static final String TIED_INDEX = "facetEdgeTied";
	private static final String HIERARCHICAL_INDEX = "facetEdgeHierarchical";
	private static final int UNIQUE_CATEGORIES = 10;
	private static final int DOCS_PER_CATEGORY = 20;

	private static List<FacetCount> baselineCountsSingleShard;
	private static List<ZuliaQuery.FacetStats> baselineStatsSingleShard;

	@Test
	@Order(1)
	public void createIndexes() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		ClientIndexConfig singleShardConfig = new ClientIndexConfig();
		singleShardConfig.addDefaultSearchField("title");
		singleShardConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		singleShardConfig.addFieldConfig(FieldConfigBuilder.createString("category").indexAs(DefaultAnalyzers.LC_KEYWORD).facet().sort());
		singleShardConfig.addFieldConfig(FieldConfigBuilder.createInt("value").index().sort());
		singleShardConfig.setIndexName(SINGLE_SHARD_INDEX);
		singleShardConfig.setNumberOfShards(1);
		singleShardConfig.setShardCommitInterval(20);
		zuliaWorkPool.createIndex(singleShardConfig);

		ClientIndexConfig multiShardConfig = new ClientIndexConfig();
		multiShardConfig.addDefaultSearchField("title");
		multiShardConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		multiShardConfig.addFieldConfig(FieldConfigBuilder.createString("category").indexAs(DefaultAnalyzers.LC_KEYWORD).facet().sort());
		multiShardConfig.addFieldConfig(FieldConfigBuilder.createInt("value").index().sort());
		multiShardConfig.setIndexName(MULTI_SHARD_INDEX);
		multiShardConfig.setNumberOfShards(3);
		multiShardConfig.setShardCommitInterval(20);
		zuliaWorkPool.createIndex(multiShardConfig);

		ClientIndexConfig tiedConfig = new ClientIndexConfig();
		tiedConfig.addDefaultSearchField("title");
		tiedConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		tiedConfig.addFieldConfig(FieldConfigBuilder.createString("category").indexAs(DefaultAnalyzers.LC_KEYWORD).facet().sort());
		tiedConfig.addFieldConfig(FieldConfigBuilder.createInt("value").index().sort());
		tiedConfig.setIndexName(TIED_INDEX);
		tiedConfig.setNumberOfShards(1);
		tiedConfig.setShardCommitInterval(20);
		zuliaWorkPool.createIndex(tiedConfig);

		ClientIndexConfig hierarchicalConfig = new ClientIndexConfig();
		hierarchicalConfig.addDefaultSearchField("title");
		hierarchicalConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		hierarchicalConfig.addFieldConfig(FieldConfigBuilder.createString("path").indexAs(DefaultAnalyzers.LC_KEYWORD).facetHierarchical().sort());
		hierarchicalConfig.addFieldConfig(FieldConfigBuilder.createInt("value").index().sort());
		hierarchicalConfig.setIndexName(HIERARCHICAL_INDEX);
		hierarchicalConfig.setNumberOfShards(1);
		hierarchicalConfig.setShardCommitInterval(20);
		zuliaWorkPool.createIndex(hierarchicalConfig);
	}

	@Test
	@Order(2)
	public void indexData() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Distinct counts per category: category_0 gets 29 docs, category_9 gets 20
		for (int cat = 0; cat < UNIQUE_CATEGORIES; cat++) {
			int docsForCategory = DOCS_PER_CATEGORY + (UNIQUE_CATEGORIES - 1 - cat);
			for (int j = 0; j < docsForCategory; j++) {
				String id = "cat" + cat + "_doc" + j;
				Document doc = new Document();
				doc.put("id", id);
				doc.put("title", "Document " + id);
				doc.put("category", "category_" + cat);
				doc.put("value", (cat + 1) * 10 + j);
				zuliaWorkPool.store(new Store(id, SINGLE_SHARD_INDEX).setResultDocument(ResultDocBuilder.from(doc)));
				zuliaWorkPool.store(new Store(id, MULTI_SHARD_INDEX).setResultDocument(ResultDocBuilder.from(doc)));
			}
		}

		// Tied index: equal doc counts, distinct sums per category
		for (int cat = 0; cat < UNIQUE_CATEGORIES; cat++) {
			for (int j = 0; j < DOCS_PER_CATEGORY; j++) {
				String id = "tied_cat" + cat + "_doc" + j;
				Document doc = new Document();
				doc.put("id", id);
				doc.put("title", "Document " + id);
				doc.put("category", "category_" + cat);
				doc.put("value", (cat + 1) * 100 + j);
				zuliaWorkPool.store(new Store(id, TIED_INDEX).setResultDocument(ResultDocBuilder.from(doc)));
			}
		}

		// Hierarchical: "a" has 3 children (x,y,z), "b" has 2 (p,q), "c" has none
		String[][] hierarchicalPaths = { { "a/x", "a/y", "a/z" }, { "b/p", "b/q" }, { "c" } };
		int docId = 0;
		for (String[] group : hierarchicalPaths) {
			for (String path : group) {
				for (int j = 0; j < 15; j++) {
					String id = "hier_" + docId;
					Document doc = new Document();
					doc.put("id", id);
					doc.put("title", "Hierarchical " + id);
					doc.put("path", path);
					doc.put("value", docId);
					zuliaWorkPool.store(new Store(id, HIERARCHICAL_INDEX).setResultDocument(ResultDocBuilder.from(doc)));
					docId++;
				}
			}
		}
	}

	@Test
	@Order(3)
	public void establishBaseline() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		baselineCountsSingleShard = getAllCountFacets(zuliaWorkPool, SINGLE_SHARD_INDEX);
		baselineStatsSingleShard = getAllStatFacets(zuliaWorkPool, SINGLE_SHARD_INDEX);
		Assertions.assertEquals(UNIQUE_CATEGORIES, baselineCountsSingleShard.size());
		Assertions.assertEquals(UNIQUE_CATEGORIES, baselineStatsSingleShard.size());
		assertCountsDescending(baselineCountsSingleShard, "baseline");
	}

	// Tests topN values across both paths: 1,4 (PQ), 5 (boundary), 9,10,11 (collect-and-sort / exceeds)
	@Test
	@Order(4)
	public void testCountFacetTopNBoundary() throws Exception {
		for (int topN : new int[] { 1, 4, 5, 9, 10, 11 }) {
			verifyCountFacetSubset(SINGLE_SHARD_INDEX, topN, baselineCountsSingleShard, "count topN=" + topN);
		}
	}

	@Test
	@Order(5)
	public void testStatFacetTopNBoundary() throws Exception {
		for (int topN : new int[] { 1, 4, 5, 9, 10, 11 }) {
			verifyStatFacetSubset(SINGLE_SHARD_INDEX, topN, baselineStatsSingleShard, "stat topN=" + topN);
		}
	}

	// Multi-shard with shardFacets=-1 across boundary values
	@Test
	@Order(6)
	public void testMultiShardAllShardFacets() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		List<FacetCount> multiBaseline = getAllCountFacets(zuliaWorkPool, MULTI_SHARD_INDEX);
		Assertions.assertEquals(UNIQUE_CATEGORIES, multiBaseline.size());

		for (int topN : new int[] { 4, 5, 9, 10, 11 }) {
			Search search = new Search(MULTI_SHARD_INDEX).setRealtime(true);
			search.addCountFacet(new CountFacet("category").setTopN(topN).setTopNShard(-1));
			search.addStat(new StatFacet("value", "category").setTopN(topN).setTopNShard(-1));
			SearchResult result = zuliaWorkPool.search(search);

			int expectedSize = Math.min(topN, UNIQUE_CATEGORIES);
			List<FacetCount> counts = result.getFacetCounts("category");
			Assertions.assertEquals(expectedSize, counts.size(), "multi-shard count topN=" + topN);
			assertCountsDescending(counts, "multi-shard topN=" + topN);
			assertCountFacetsMatchBaseline(counts, multiBaseline, "multi-shard topN=" + topN);

			List<ZuliaQuery.FacetStats> stats = result.getFacetFieldStat("value", "category");
			Assertions.assertEquals(expectedSize, stats.size(), "multi-shard stat topN=" + topN);
		}
	}

	// Cross-path: PQ result (topN=4) must exactly match the top-4 of collect-and-sort (topN=10)
	@Test
	@Order(7)
	public void testCrossPathConsistency() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		Search pqSearch = new Search(SINGLE_SHARD_INDEX).setRealtime(true);
		pqSearch.addCountFacet(new CountFacet("category").setTopN(4));
		pqSearch.addStat(new StatFacet("value", "category").setTopN(4));

		Search sortSearch = new Search(SINGLE_SHARD_INDEX).setRealtime(true);
		sortSearch.addCountFacet(new CountFacet("category").setTopN(10));
		sortSearch.addStat(new StatFacet("value", "category").setTopN(10));

		SearchResult pqResult = zuliaWorkPool.search(pqSearch);
		SearchResult sortResult = zuliaWorkPool.search(sortSearch);

		List<FacetCount> pqCounts = pqResult.getFacetCounts("category");
		List<FacetCount> sortCounts = sortResult.getFacetCounts("category");
		for (int i = 0; i < 4; i++) {
			Assertions.assertEquals(pqCounts.get(i).getFacet(), sortCounts.get(i).getFacet(), "cross-path count mismatch at " + i);
			Assertions.assertEquals(pqCounts.get(i).getCount(), sortCounts.get(i).getCount(), "cross-path count value mismatch at " + i);
		}

		List<ZuliaQuery.FacetStats> pqStats = pqResult.getFacetFieldStat("value", "category");
		List<ZuliaQuery.FacetStats> sortStats = sortResult.getFacetFieldStat("value", "category");
		for (int i = 0; i < 4; i++) {
			Assertions.assertEquals(pqStats.get(i).getFacet(), sortStats.get(i).getFacet(), "cross-path stat mismatch at " + i);
			Assertions.assertEquals(pqStats.get(i).getSum(), sortStats.get(i).getSum(), "cross-path stat sum mismatch at " + i);
		}
	}

	// Tied counts: tiebreaker (ordinal) must be consistent across PQ and collect-and-sort
	@Test
	@Order(8)
	public void testTiedCountsTiebreaker() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Verify all counts are actually tied
		Search allSearch = new Search(TIED_INDEX).setRealtime(true);
		allSearch.addCountFacet(new CountFacet("category").setTopN(-1));
		List<FacetCount> allCounts = zuliaWorkPool.search(allSearch).getFacetCounts("category");
		for (FacetCount fc : allCounts) {
			Assertions.assertEquals(DOCS_PER_CATEGORY, fc.getCount(), "tied count mismatch for '" + fc.getFacet() + "'");
		}

		// topN=4 (PQ) vs topN=5 (collect-and-sort) — first 4 must match
		List<FacetCount> pqResults = zuliaWorkPool
				.search(new Search(TIED_INDEX).setRealtime(true).addCountFacet(new CountFacet("category").setTopN(4))).getFacetCounts("category");
		List<FacetCount> sortResults = zuliaWorkPool
				.search(new Search(TIED_INDEX).setRealtime(true).addCountFacet(new CountFacet("category").setTopN(5))).getFacetCounts("category");
		for (int i = 0; i < 4; i++) {
			Assertions.assertEquals(pqResults.get(i).getFacet(), sortResults.get(i).getFacet(), "tied tiebreaker mismatch at " + i);
		}
	}

	// NRT refresh: new facet values → new ShardReader → cache must reflect updated cardinality
	@Test
	@Order(9)
	public void testNrtRefreshChangingCardinality() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Add 3 new categories
		for (int cat = 0; cat < 3; cat++) {
			for (int j = 0; j < 5; j++) {
				String id = "new_cat" + cat + "_doc" + j;
				Document doc = new Document();
				doc.put("id", id);
				doc.put("title", "New " + id);
				doc.put("category", "new_category_" + cat);
				doc.put("value", 1000 + cat * 10 + j);
				zuliaWorkPool.store(new Store(id, SINGLE_SHARD_INDEX).setResultDocument(ResultDocBuilder.from(doc)));
			}
		}

		// Verify new cardinality (13). New threshold is 13/2 = 6.
		Search afterSearch = new Search(SINGLE_SHARD_INDEX).setRealtime(true);
		afterSearch.addCountFacet(new CountFacet("category").setTopN(-1));
		List<FacetCount> after = zuliaWorkPool.search(afterSearch).getFacetCounts("category");
		Assertions.assertEquals(13, after.size());

		// topN=5 (PQ: 5 < 6) vs topN=7 (collect-and-sort: 7 >= 6) — cross-path check
		List<FacetCount> pqResult = zuliaWorkPool
				.search(new Search(SINGLE_SHARD_INDEX).setRealtime(true).addCountFacet(new CountFacet("category").setTopN(5))).getFacetCounts("category");
		List<FacetCount> sortResult = zuliaWorkPool
				.search(new Search(SINGLE_SHARD_INDEX).setRealtime(true).addCountFacet(new CountFacet("category").setTopN(7))).getFacetCounts("category");
		Assertions.assertEquals(5, pqResult.size());
		Assertions.assertEquals(7, sortResult.size());
		for (int i = 0; i < 5; i++) {
			Assertions.assertEquals(pqResult.get(i).getFacet(), sortResult.get(i).getFacet(), "NRT cross-path mismatch at " + i);
		}
	}

	// No-match filter → empty facet results from both paths
	@Test
	@Order(10)
	public void testNoMatchQuery() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		for (int topN : new int[] { 4, 5, 10 }) {
			Search search = new Search(SINGLE_SHARD_INDEX).setRealtime(true);
			search.addQuery(new FilterQuery("category:nonexistent_value_xyz"));
			search.addCountFacet(new CountFacet("category").setTopN(topN));
			search.addStat(new StatFacet("value", "category").setTopN(topN));
			SearchResult result = zuliaWorkPool.search(search);

			Assertions.assertEquals(0, result.getTotalHits());
			Assertions.assertEquals(0, result.getFacetCounts("category").size(), "no-match count topN=" + topN);
			Assertions.assertEquals(0, result.getFacetFieldStat("value", "category").size(), "no-match stat topN=" + topN);
		}
	}

	// Concurrent queries stressing the ConcurrentHashMap dimension child count cache
	@Test
	@Order(11)
	public void testConcurrentFacetQueries() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		int threadCount = 10;
		int iterationsPerThread = 20;

		CyclicBarrier barrier = new CyclicBarrier(threadCount);
		ExecutorService executor = Executors.newFixedThreadPool(threadCount);
		List<Future<Void>> futures = new ArrayList<>();

		for (int t = 0; t < threadCount; t++) {
			final int threadId = t;
			futures.add(executor.submit(() -> {
				barrier.await();
				for (int i = 0; i < iterationsPerThread; i++) {
					// Alternate between PQ (topN=4) and collect-and-sort (topN=7)
					int topN = (threadId + i) % 2 == 0 ? 4 : 7;
					Search search = new Search(SINGLE_SHARD_INDEX).setRealtime(true);
					search.addCountFacet(new CountFacet("category").setTopN(topN));

					List<FacetCount> counts = zuliaWorkPool.search(search).getFacetCounts("category");
					Assertions.assertTrue(counts.size() <= topN, "thread " + threadId + ": size " + counts.size() + " > topN " + topN);
					assertCountsDescending(counts, "thread " + threadId);
				}
				return null;
			}));
		}

		for (Future<Void> future : futures) {
			future.get();
		}
		executor.shutdown();
	}

	// Hierarchical: per-dimension cardinality differs at each path level
	@Test
	@Order(12)
	public void testHierarchicalFacetPaths() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Top-level has 3 children (a, b, c)
		List<FacetCount> allTopLevel = zuliaWorkPool
				.search(new Search(HIERARCHICAL_INDEX).setRealtime(true).addCountFacet(new CountFacet("path").setTopN(-1))).getFacetCounts("path");
		Assertions.assertEquals(3, allTopLevel.size());
		for (int topN : new int[] { 1, 2, 3, 5 }) {
			List<FacetCount> result = zuliaWorkPool
					.search(new Search(HIERARCHICAL_INDEX).setRealtime(true).addCountFacet(new CountFacet("path").setTopN(topN))).getFacetCounts("path");
			Assertions.assertEquals(Math.min(topN, 3), result.size(), "top-level topN=" + topN);
		}

		// Sub-path "a" has 3 children (x, y, z) — different dimOrd than top-level
		List<FacetCount> aChildren = zuliaWorkPool
				.search(new Search(HIERARCHICAL_INDEX).setRealtime(true).addCountFacet(new CountFacet("path", "a").setTopN(-1)))
				.getFacetCountsForPath("path", "a");
		Assertions.assertEquals(3, aChildren.size());
		for (int topN : new int[] { 1, 2, 3, 5 }) {
			List<FacetCount> result = zuliaWorkPool
					.search(new Search(HIERARCHICAL_INDEX).setRealtime(true).addCountFacet(new CountFacet("path", "a").setTopN(topN)))
					.getFacetCountsForPath("path", "a");
			Assertions.assertEquals(Math.min(topN, 3), result.size(), "sub-path 'a' topN=" + topN);
		}

		// Sub-path "b" has 2 children (p, q) — different cardinality than "a"
		List<FacetCount> bChildren = zuliaWorkPool
				.search(new Search(HIERARCHICAL_INDEX).setRealtime(true).addCountFacet(new CountFacet("path", "b").setTopN(-1)))
				.getFacetCountsForPath("path", "b");
		Assertions.assertEquals(2, bChildren.size());
		for (int topN : new int[] { 1, 2, 3 }) {
			List<FacetCount> result = zuliaWorkPool
					.search(new Search(HIERARCHICAL_INDEX).setRealtime(true).addCountFacet(new CountFacet("path", "b").setTopN(topN)))
					.getFacetCountsForPath("path", "b");
			Assertions.assertEquals(Math.min(topN, 2), result.size(), "sub-path 'b' topN=" + topN);
		}
	}

	// --- Helpers ---

	private List<FacetCount> getAllCountFacets(ZuliaWorkPool zuliaWorkPool, String indexName) throws Exception {
		return zuliaWorkPool.search(new Search(indexName).setRealtime(true).addCountFacet(new CountFacet("category").setTopN(-1)))
				.getFacetCounts("category");
	}

	private List<ZuliaQuery.FacetStats> getAllStatFacets(ZuliaWorkPool zuliaWorkPool, String indexName) throws Exception {
		return zuliaWorkPool.search(new Search(indexName).setRealtime(true).addStat(new StatFacet("value", "category").setTopN(-1)))
				.getFacetFieldStat("value", "category");
	}

	private void verifyCountFacetSubset(String indexName, int topN, List<FacetCount> baseline, String description) throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		List<FacetCount> facetCounts = zuliaWorkPool
				.search(new Search(indexName).setRealtime(true).addCountFacet(new CountFacet("category").setTopN(topN))).getFacetCounts("category");
		int expectedSize = Math.min(topN, UNIQUE_CATEGORIES);
		Assertions.assertEquals(expectedSize, facetCounts.size(), description);
		assertCountsDescending(facetCounts, description);
		assertCountFacetsMatchBaseline(facetCounts, baseline, description);
	}

	private void verifyStatFacetSubset(String indexName, int topN, List<ZuliaQuery.FacetStats> baseline, String description) throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		List<ZuliaQuery.FacetStats> statFacets = zuliaWorkPool
				.search(new Search(indexName).setRealtime(true).addStat(new StatFacet("value", "category").setTopN(topN)))
				.getFacetFieldStat("value", "category");
		int expectedSize = Math.min(topN, UNIQUE_CATEGORIES);
		Assertions.assertEquals(expectedSize, statFacets.size(), description);
		assertStatFacetsMatchBaseline(statFacets, baseline, description);
	}

	private void assertCountsDescending(List<FacetCount> facetCounts, String description) {
		for (int i = 1; i < facetCounts.size(); i++) {
			Assertions.assertTrue(facetCounts.get(i - 1).getCount() >= facetCounts.get(i).getCount(),
					description + ": not descending at " + i);
		}
	}

	private void assertCountFacetsMatchBaseline(List<FacetCount> actual, List<FacetCount> baseline, String description) {
		Set<String> baselineTopFacets = baseline.stream().limit(actual.size()).map(FacetCount::getFacet).collect(Collectors.toSet());
		for (FacetCount fc : actual) {
			Assertions.assertTrue(baselineTopFacets.contains(fc.getFacet()), description + ": '" + fc.getFacet() + "' not in baseline top");
			long baselineCount = baseline.stream().filter(b -> b.getFacet().equals(fc.getFacet())).findFirst().orElseThrow().getCount();
			Assertions.assertEquals(baselineCount, fc.getCount(), description + ": count mismatch for '" + fc.getFacet() + "'");
		}
	}

	private void assertStatFacetsMatchBaseline(List<ZuliaQuery.FacetStats> actual, List<ZuliaQuery.FacetStats> baseline, String description) {
		Set<String> baselineTopFacets = baseline.stream().limit(actual.size()).map(ZuliaQuery.FacetStats::getFacet).collect(Collectors.toSet());
		for (ZuliaQuery.FacetStats fs : actual) {
			Assertions.assertTrue(baselineTopFacets.contains(fs.getFacet()), description + ": '" + fs.getFacet() + "' not in baseline top");
			ZuliaQuery.FacetStats baselineStat = baseline.stream().filter(b -> b.getFacet().equals(fs.getFacet())).findFirst().orElseThrow();
			Assertions.assertEquals(baselineStat.getDocCount(), fs.getDocCount(), description + ": docCount mismatch for '" + fs.getFacet() + "'");
			Assertions.assertEquals(baselineStat.getSum(), fs.getSum(), description + ": sum mismatch for '" + fs.getFacet() + "'");
		}
	}
}
