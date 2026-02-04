package io.zulia.server.search;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.Search;
import io.zulia.client.command.builder.Sort;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.SearchResult;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.server.test.node.shared.NodeExtension;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.RegisterExtension;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class QueryCombinerTest {

	@RegisterExtension
	static final NodeExtension nodeExtension = new NodeExtension(3);

	public static final String TEST_INDEX = "queryCombinerTest";
	private static final int SHARD_COUNT = 5;
	private static final int DOC_COUNT = 100;

	@Test
	@Order(1)
	public void createIndex() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("title");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("title").indexAs(DefaultAnalyzers.STANDARD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createInt("rank").index().sort());
		indexConfig.setIndexName(TEST_INDEX);
		indexConfig.setNumberOfShards(SHARD_COUNT);
		indexConfig.setShardCommitInterval(20);

		zuliaWorkPool.createIndex(indexConfig);
	}

	@Test
	@Order(2)
	public void indexData() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Index documents with sequential ranks for predictable ordering
		for (int i = 0; i < DOC_COUNT; i++) {
			Document doc = new Document();
			doc.put("id", String.valueOf(i));
			doc.put("title", "Document " + i);
			doc.put("rank", i);

			Store store = new Store(String.valueOf(i), TEST_INDEX);
			store.setResultDocument(ResultDocBuilder.from(doc));
			zuliaWorkPool.store(store);
		}
	}

	@Test
	@Order(3)
	public void testPagination() throws Exception {
		testPaginationWithConcurrency(null);
		testPaginationWithConcurrency(2);
	}

	private void testPaginationWithConcurrency(Integer concurrency) throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Test 1: Basic pagination - get first 10
		Search search = new Search(TEST_INDEX).setRealtime(true);
		if (concurrency != null) {
			search.setConcurrency(concurrency);
		}
		search.addSort(new Sort("rank"));
		search.setAmount(10);

		SearchResult result = zuliaWorkPool.search(search);
		Assertions.assertEquals(DOC_COUNT, result.getTotalHits());
		Assertions.assertEquals(10, result.getCompleteResults().size());

		// Verify first 10 documents (rank 0-9)
		for (int i = 0; i < 10; i++) {
			Document doc = result.getCompleteResults().get(i).getDocument();
			Assertions.assertEquals(i, doc.getInteger("rank"));
		}

		// Test 2: Pagination with start offset - get docs 20-29
		search = new Search(TEST_INDEX);
		if (concurrency != null) {
			search.setConcurrency(concurrency);
		}
		search.addSort(new Sort("rank"));
		search.setStart(20);
		search.setAmount(10);

		result = zuliaWorkPool.search(search);
		Assertions.assertEquals(DOC_COUNT, result.getTotalHits());
		Assertions.assertEquals(10, result.getCompleteResults().size());

		// Verify documents 20-29
		for (int i = 0; i < 10; i++) {
			Document doc = result.getCompleteResults().get(i).getDocument();
			Assertions.assertEquals(20 + i, doc.getInteger("rank"));
		}

		// Test 3: Pagination near end - get docs 95-99
		search = new Search(TEST_INDEX);
		if (concurrency != null) {
			search.setConcurrency(concurrency);
		}
		search.addSort(new Sort("rank"));
		search.setStart(95);
		search.setAmount(10);

		result = zuliaWorkPool.search(search);
		Assertions.assertEquals(DOC_COUNT, result.getTotalHits());
		Assertions.assertEquals(5, result.getCompleteResults().size()); // Only 5 results left

		// Verify documents 95-99
		for (int i = 0; i < 5; i++) {
			Document doc = result.getCompleteResults().get(i).getDocument();
			Assertions.assertEquals(95 + i, doc.getInteger("rank"));
		}

		// Test 4: Start beyond results - should return empty
		search = new Search(TEST_INDEX);
		if (concurrency != null) {
			search.setConcurrency(concurrency);
		}
		search.addSort(new Sort("rank"));
		search.setStart(200);
		search.setAmount(10);

		result = zuliaWorkPool.search(search);
		Assertions.assertEquals(DOC_COUNT, result.getTotalHits());
		Assertions.assertEquals(0, result.getCompleteResults().size());
	}

	@Test
	@Order(4)
	public void testResultMerging() throws Exception {
		testResultMergingWithConcurrency(null);
		testResultMergingWithConcurrency(3);
	}

	private void testResultMergingWithConcurrency(Integer concurrency) throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// With 5 shards, results should be properly merged and sorted
		Search search = new Search(TEST_INDEX);
		if (concurrency != null) {
			search.setConcurrency(concurrency);
		}
		search.addSort(new Sort("rank"));
		search.setAmount(50);

		SearchResult result = zuliaWorkPool.search(search);
		Assertions.assertEquals(DOC_COUNT, result.getTotalHits());
		Assertions.assertEquals(50, result.getCompleteResults().size());

		// Verify results are properly sorted across all shards
		int previousRank = -1;
		for (int i = 0; i < 50; i++) {
			Document doc = result.getCompleteResults().get(i).getDocument();
			int rank = doc.getInteger("rank");
			Assertions.assertTrue(rank > previousRank,
					"Results should be sorted by rank: " + previousRank + " should be < " + rank);
			previousRank = rank;
		}
	}

	@Test
	@Order(5)
	public void testDescendingSort() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Test descending sort with pagination
		Search search = new Search(TEST_INDEX);
		search.addSort(new Sort("rank").descending());
		search.setStart(10);
		search.setAmount(10);

		SearchResult result = zuliaWorkPool.search(search);
		Assertions.assertEquals(DOC_COUNT, result.getTotalHits());
		Assertions.assertEquals(10, result.getCompleteResults().size());

		// Should get docs with ranks 89, 88, 87, ... 80 (descending from 99, skip first 10)
		for (int i = 0; i < 10; i++) {
			Document doc = result.getCompleteResults().get(i).getDocument();
			Assertions.assertEquals(89 - i, doc.getInteger("rank"));
		}
	}

	@Test
	@Order(6)
	public void testCachingStats() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Run same query twice to test caching
		Search search = new Search(TEST_INDEX);
		search.addSort(new Sort("rank"));
		search.setAmount(10);

		SearchResult result1 = zuliaWorkPool.search(search);
		SearchResult result2 = zuliaWorkPool.search(search);

		// Both should return same results
		Assertions.assertEquals(result1.getTotalHits(), result2.getTotalHits());
		Assertions.assertEquals(result1.getCompleteResults().size(), result2.getCompleteResults().size());

		// Second query might have cached shards
		Assertions.assertTrue(result2.getShardsCached() >= 0);
		Assertions.assertEquals(SHARD_COUNT, result2.getShardsQueried());
	}
}
