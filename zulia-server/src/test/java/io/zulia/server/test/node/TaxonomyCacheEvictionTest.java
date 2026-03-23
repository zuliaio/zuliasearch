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
 * Tests that the taxonomy writer cache correctly handles eviction when the number
 * of unique facet values exceeds the configured maxFacetsCachedPerDimension.
 * <p>
 * The bug scenario: when a category is added to the taxonomy index and cached,
 * then evicted from the LRU cache, the taxonomy writer's internal reader may be
 * stale and unable to find the category on disk. Without a reader refresh triggered
 * by put() returning true, the writer assigns a duplicate ordinal for the same category.
 * <p>
 * To trigger this reliably:
 * - Cache size is set to 2 (minimum, forces constant eviction)
 * - shardCommitInterval is very high (prevents commits from refreshing the reader)
 * - Documents are stored in shuffled order across many rounds so categories are
 *   frequently reused after being evicted from cache
 * - Multiple threads blast stores concurrently
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TaxonomyCacheEvictionTest {

	// Minimum cache size to force constant eviction
	private static final int MAX_FACETS_CACHED_PER_DIMENSION = 2;

	@RegisterExtension
	static final NodeExtension nodeExtension = new NodeExtension(1, config -> config.setMaxFacetsCachedPerDimension(MAX_FACETS_CACHED_PER_DIMENSION));

	private static final String INDEX_NAME = "taxonomyCacheEvictionTest";

	// Many more unique values than cache can hold
	private static final int UNIQUE_VALUES = 200;

	// Multiple rounds of reuse to maximize chance of hitting stale reader
	private static final int ROUNDS = 5;

	private static final int THREAD_COUNT = 16;

	@Test
	@Order(1)
	public void createIndex() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("title");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("category").facet());
		indexConfig.setIndexName(INDEX_NAME);
		indexConfig.setNumberOfShards(1);
		// Very high commit interval to prevent commits during the store burst
		// This keeps the taxonomy reader stale, which is required to trigger the bug
		indexConfig.setShardCommitInterval(999999);

		zuliaWorkPool.createIndex(indexConfig);
	}

	@Test
	@Order(2)
	public void concurrentStoreWithCacheThrashing() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Build document list: each category appears ROUNDS times
		// Shuffle to maximize cache thrashing - categories are reused after eviction
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

		Search search = new Search(INDEX_NAME).setRealtime(true);
		search.addCountFacet(new CountFacet("category").setTopN(-1));

		SearchResult result = zuliaWorkPool.search(search);

		int totalDocs = UNIQUE_VALUES * ROUNDS;

		Assertions.assertEquals(totalDocs, result.getTotalHits(), "Total document count should match");

		List<FacetCount> categoryCounts = result.getFacetCounts("category");

		// If duplicate ordinals were assigned, the same category name would appear
		// multiple times in the facet counts, or counts would be split across ordinals
		Assertions.assertEquals(UNIQUE_VALUES, categoryCounts.size(),
				"Should have exactly " + UNIQUE_VALUES + " unique category facet values. " + "More values indicate duplicate ordinals were assigned for the same category. "
						+ "Actual count: " + categoryCounts.size());

		Map<String, Long> categoryMap = categoryCounts.stream().collect(Collectors.toMap(FacetCount::getFacet, FacetCount::getCount));
		for (int i = 0; i < UNIQUE_VALUES; i++) {
			String category = "cat_" + i;
			Long count = categoryMap.get(category);
			Assertions.assertNotNull(count, "Category " + category + " should exist in facet counts");
			Assertions.assertEquals(ROUNDS, count.intValue(),
					"Category " + category + " should have exactly " + ROUNDS + " docs but had " + count + ". " + "Miscount suggests ordinal duplication split the count.");
		}

		// Sum all facet counts - should equal total docs
		// If duplicate ordinals exist, sum may exceed total docs
		long facetSum = categoryCounts.stream().mapToLong(FacetCount::getCount).sum();
		Assertions.assertEquals(totalDocs, facetSum, "Sum of all facet counts should equal total docs. " + "A higher sum indicates duplicate ordinals inflating counts.");
	}
}
