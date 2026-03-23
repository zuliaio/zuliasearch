package io.zulia.server.test.node;

import io.zulia.client.command.Store;
import io.zulia.client.command.builder.CountFacet;
import io.zulia.client.command.builder.Search;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.SearchResult;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.fields.FieldConfigBuilder;
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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Tests that the taxonomy writer cache correctly handles eviction with throttled reader
 * refreshes. Unlike {@link TaxonomyCacheEvictionTest} which uses cache size 2 (forcing
 * a refresh on every eviction), this test uses cache size 32 so that
 * evictionsBetweenRefreshes = max(1, 32/4) = 8, exercising the batched refresh path
 * where most evictions do NOT trigger a reader refresh.
 * <p>
 * With 500 unique values across 5 rounds and cache size 32, there are many more evictions
 * than refreshes per dimension. This verifies no duplicate ordinals are assigned when
 * refreshes are throttled.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TaxonomyCacheThrottledRefreshTest {

	private static final int MAX_FACETS_CACHED_PER_DIMENSION = 32;

	@RegisterExtension
	static final NodeExtension nodeExtension = new NodeExtension(1, config -> config.setMaxFacetsCachedPerDimension(MAX_FACETS_CACHED_PER_DIMENSION));

	private static final String INDEX_NAME = "taxonomyCacheThrottledRefreshTest";

	// Many more unique values than cache size to force heavy eviction with throttled refreshes
	private static final int UNIQUE_VALUES = 500;

	private static final int ROUNDS = 5;

	private static final int THREAD_COUNT = 16;

	@Test
	@Order(1)
	public void createIndex() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("title");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("category").facet());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("region").facet());
		indexConfig.setIndexName(INDEX_NAME);
		indexConfig.setNumberOfShards(1);
		indexConfig.setShardCommitInterval(999999);

		zuliaWorkPool.createIndex(indexConfig);
	}

	@Test
	@Order(2)
	public void concurrentStoreWithThrottledRefresh() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		List<int[]> docSpecs = new ArrayList<>();
		int docId = 0;
		for (int round = 0; round < ROUNDS; round++) {
			for (int v = 0; v < UNIQUE_VALUES; v++) {
				docSpecs.add(new int[] { docId++, v });
			}
		}
		Collections.shuffle(docSpecs);

		AtomicInteger idx = new AtomicInteger(0);
		CyclicBarrier barrier = new CyclicBarrier(THREAD_COUNT);
		ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);
		List<Future<Void>> futures = new ArrayList<>();

		for (int t = 0; t < THREAD_COUNT; t++) {
			futures.add(executor.submit(() -> {
				barrier.await();
				int i;
				while ((i = idx.getAndIncrement()) < docSpecs.size()) {
					int[] spec = docSpecs.get(i);
					int id = spec[0];
					int valueIndex = spec[1];

					Document mongoDocument = new Document();
					mongoDocument.put("id", String.valueOf(id));
					mongoDocument.put("category", "cat_" + valueIndex);
					mongoDocument.put("region", "region_" + (valueIndex % 50));

					Store s = new Store(String.valueOf(id), INDEX_NAME);
					s.setResultDocument(ResultDocBuilder.newBuilder().setDocument(mongoDocument));
					zuliaWorkPool.store(s);
				}
				return null;
			}));
		}

		for (Future<Void> future : futures) {
			future.get();
		}
		executor.shutdown();
	}

	@Test
	@Order(3)
	public void verifyNoDuplicateOrdinals() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		int totalDocs = UNIQUE_VALUES * ROUNDS;

		// Verify category facet (500 unique values, cache size 32 → heavy eviction with throttled refresh)
		{
			Search search = new Search(INDEX_NAME).setRealtime(true);
			search.addCountFacet(new CountFacet("category").setTopN(-1));

			SearchResult result = zuliaWorkPool.search(search);

			Assertions.assertEquals(totalDocs, result.getTotalHits(), "Total document count should match");

			List<FacetCount> categoryCounts = result.getFacetCounts("category");

			Assertions.assertEquals(UNIQUE_VALUES, categoryCounts.size(),
					"Should have exactly " + UNIQUE_VALUES + " unique category facet values. " + "More values indicate duplicate ordinals were assigned. "
							+ "Actual count: " + categoryCounts.size());

			Map<String, Long> categoryMap = categoryCounts.stream().collect(Collectors.toMap(FacetCount::getFacet, FacetCount::getCount));
			for (int i = 0; i < UNIQUE_VALUES; i++) {
				String category = "cat_" + i;
				Long count = categoryMap.get(category);
				Assertions.assertNotNull(count, "Category " + category + " should exist in facet counts");
				Assertions.assertEquals(ROUNDS, count.intValue(),
						"Category " + category + " should have exactly " + ROUNDS + " docs but had " + count);
			}

			long facetSum = categoryCounts.stream().mapToLong(FacetCount::getCount).sum();
			Assertions.assertEquals(totalDocs, facetSum, "Sum of all category facet counts should equal total docs");
		}

		// Verify region facet (50 unique values, cache size 32 → moderate eviction with throttled refresh)
		{
			Search search = new Search(INDEX_NAME).setRealtime(true);
			search.addCountFacet(new CountFacet("region").setTopN(-1));

			SearchResult result = zuliaWorkPool.search(search);

			List<FacetCount> regionCounts = result.getFacetCounts("region");

			int uniqueRegions = 50;
			Assertions.assertEquals(uniqueRegions, regionCounts.size(),
					"Should have exactly " + uniqueRegions + " unique region facet values. " + "Actual count: " + regionCounts.size());

			long regionSum = regionCounts.stream().mapToLong(FacetCount::getCount).sum();
			Assertions.assertEquals(totalDocs, regionSum, "Sum of all region facet counts should equal total docs");
		}
	}
}
