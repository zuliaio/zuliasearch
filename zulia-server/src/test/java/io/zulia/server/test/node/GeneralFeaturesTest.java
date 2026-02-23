package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.BatchDelete;
import io.zulia.client.command.BatchFetch;
import io.zulia.client.command.DeleteFromIndex;
import io.zulia.client.command.Fetch;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.CountFacet;
import io.zulia.client.command.builder.FilterQuery;
import io.zulia.client.command.builder.NumericStat;
import io.zulia.client.command.builder.Search;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.BatchFetchResult;
import io.zulia.client.result.FetchResult;
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

import java.util.List;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class GeneralFeaturesTest {

	@RegisterExtension
	static final NodeExtension nodeExtension = new NodeExtension(3);

	public static final String INDEX_NAME = "generalFeatures";

	private static final int UNIQUE_DOCS = 5;

	@Test
	@Order(1)
	public void createIndex() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("title");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("title").indexAs(DefaultAnalyzers.STANDARD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("category").indexAs(DefaultAnalyzers.STANDARD).facet());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("type").indexAs(DefaultAnalyzers.LC_KEYWORD).facet());
		indexConfig.addFieldConfig(FieldConfigBuilder.createDouble("rating").index().sort());
		indexConfig.setIndexName(INDEX_NAME);
		indexConfig.setNumberOfShards(1);
		indexConfig.setShardCommitInterval(20);

		zuliaWorkPool.createIndex(indexConfig);
	}

	@Test
	@Order(2)
	public void index() throws Exception {
		indexRecord(1, "Java Programming", "tech", "book", 4.5);
		indexRecord(2, "Python Basics", "tech", "book", 3.8);
		indexRecord(3, "Cooking Guide", "food", "article", 4.2);
		indexRecord(4, "Travel Tips", "travel", "article", 3.5);
		indexRecord(5, "Data Science", "tech", "book", 4.9);
	}

	private void indexRecord(int id, String title, String category, String type, double rating) throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		String uniqueId = String.valueOf(id);

		Document mongoDocument = new Document();
		mongoDocument.put("id", uniqueId);
		mongoDocument.put("title", title);
		mongoDocument.put("category", category);
		mongoDocument.put("type", type);
		mongoDocument.put("rating", rating);

		Store s = new Store(uniqueId, INDEX_NAME);
		s.setResultDocument(ResultDocBuilder.newBuilder().setDocument(mongoDocument));
		zuliaWorkPool.store(s);
	}

	@Test
	@Order(3)
	public void documentFieldFilteringTest() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Whitelist: fetch only specific fields
		Fetch fetch = new Fetch("1", INDEX_NAME);
		fetch.addDocumentField("title");
		fetch.setRealtime(true);
		FetchResult fetchResult = zuliaWorkPool.fetch(fetch);
		Document doc = fetchResult.getDocument();
		Assertions.assertNotNull(doc.getString("title"));
		Assertions.assertNull(doc.getString("category"));
		Assertions.assertNull(doc.get("rating"));
		Assertions.assertNull(doc.getString("id"));

		// Mask: exclude specific fields
		Fetch fetchMasked = new Fetch("1", INDEX_NAME);
		fetchMasked.addDocumentMaskedField("rating");
		fetchMasked.addDocumentMaskedField("category");
		fetchMasked.setRealtime(true);
		FetchResult maskedResult = zuliaWorkPool.fetch(fetchMasked);
		Document maskedDoc = maskedResult.getDocument();
		Assertions.assertNotNull(maskedDoc.getString("title"));
		Assertions.assertNotNull(maskedDoc.getString("id"));
		Assertions.assertNull(maskedDoc.get("rating"));
		Assertions.assertNull(maskedDoc.getString("category"));

		// Search with document fields (whitelist)
		Search search = new Search(INDEX_NAME).setAmount(10).setRealtime(true);
		search.addDocumentFields("title", "rating");
		SearchResult searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(UNIQUE_DOCS, searchResult.getTotalHits());
		for (Document searchDoc : searchResult.getDocuments()) {
			Assertions.assertNotNull(searchDoc.getString("title"));
			Assertions.assertNotNull(searchDoc.get("rating"));
			Assertions.assertNull(searchDoc.getString("category"));
			Assertions.assertNull(searchDoc.getString("id"));
		}

		// Search with masked fields (blacklist)
		Search searchMasked = new Search(INDEX_NAME).setAmount(10);
		searchMasked.addDocumentMaskedField("rating");
		SearchResult maskedSearchResult = zuliaWorkPool.search(searchMasked);
		Assertions.assertEquals(UNIQUE_DOCS, maskedSearchResult.getTotalHits());
		for (Document searchDoc : maskedSearchResult.getDocuments()) {
			Assertions.assertNotNull(searchDoc.getString("title"));
			Assertions.assertNotNull(searchDoc.getString("id"));
			Assertions.assertNull(searchDoc.get("rating"));
		}
	}

	@Test
	@Order(4)
	public void batchFetchTest() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Batch fetch by unique IDs using custom Fetch objects with realtime
		BatchFetch batchFetch = new BatchFetch();
		for (String id : List.of("1", "3", "5")) {
			Fetch f = new Fetch(id, INDEX_NAME);
			f.setRealtime(true);
			batchFetch.addFetches(List.of(f));
		}
		BatchFetchResult batchResult = zuliaWorkPool.batchFetch(batchFetch);
		List<FetchResult> results = batchResult.getFetchResults();
		Assertions.assertEquals(3, results.size());
		Assertions.assertTrue(results.stream().allMatch(FetchResult::hasResultDocument));

		// Batch fetch from search results
		Search search = new Search(INDEX_NAME).setAmount(10).setRealtime(true);
		search.addQuery(new FilterQuery("category:tech"));
		SearchResult searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(3, searchResult.getTotalHits());

		BatchFetch batchFromSearch = new BatchFetch().addFetchDocumentsFromResults(searchResult);
		BatchFetchResult searchBatchResult = zuliaWorkPool.batchFetch(batchFromSearch);
		List<FetchResult> searchFetchResults = searchBatchResult.getFetchResults();
		Assertions.assertEquals(3, searchFetchResults.size());

		// Convenience method: batch fetch by IDs (works after data is committed)
		BatchFetch batchById = new BatchFetch();
		batchById.addFetchDocumentsFromUniqueIds(List.of("1", "2", "3", "4", "5"), INDEX_NAME);
		BatchFetchResult allResult = zuliaWorkPool.batchFetch(batchById);
		Assertions.assertEquals(UNIQUE_DOCS, allResult.getFetchResults().size());
	}

	@Test
	@Order(5)
	public void batchDeleteTest() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Index extra documents to delete
		indexRecord(100, "To Be Deleted 1", "temp", "article", 1.0);
		indexRecord(101, "To Be Deleted 2", "temp", "article", 1.0);
		indexRecord(102, "To Be Deleted 3", "temp", "article", 1.0);

		Search search = new Search(INDEX_NAME).setRealtime(true);
		SearchResult searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(UNIQUE_DOCS + 3, searchResult.getTotalHits());

		// Batch delete specific IDs
		BatchDelete batchDelete = new BatchDelete();
		batchDelete.addDelete(new DeleteFromIndex("100", INDEX_NAME));
		batchDelete.addDelete(new DeleteFromIndex("101", INDEX_NAME));
		batchDelete.addDelete(new DeleteFromIndex("102", INDEX_NAME));
		zuliaWorkPool.batchDelete(batchDelete);

		// Batch delete from search results
		indexRecord(200, "Delete via search 1", "temp2", "article", 1.0);
		indexRecord(201, "Delete via search 2", "temp2", "article", 1.0);

		search = new Search(INDEX_NAME).setAmount(10).setRealtime(true);
		search.addQuery(new FilterQuery("category:temp2"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(2, searchResult.getTotalHits());

		BatchDelete deleteFromResults = new BatchDelete().deleteDocumentFromQueryResult(searchResult);
		zuliaWorkPool.batchDelete(deleteFromResults);

		// Verify the original documents remain after all batch deletes
		search = new Search(INDEX_NAME).setRealtime(true);
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(UNIQUE_DOCS, searchResult.getTotalHits());
	}

	@Test
	@Order(6)
	public void clearIndexTest() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Verify documents exist
		Search search = new Search(INDEX_NAME).setRealtime(true);
		SearchResult searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(UNIQUE_DOCS, searchResult.getTotalHits());

		// Clear the index
		zuliaWorkPool.clearIndex(INDEX_NAME);

		// Verify all documents are gone
		search = new Search(INDEX_NAME).setRealtime(true);
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(0, searchResult.getTotalHits());

		// Index config should still exist
		Assertions.assertNotNull(zuliaWorkPool.getIndexConfig(INDEX_NAME));
	}

	@Test
	@Order(7)
	public void optimizeAndReindexTest() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Re-add documents after clear
		indexRecord(1, "Java Programming", "tech", "book", 4.5);
		indexRecord(2, "Python Basics", "tech", "book", 3.8);
		indexRecord(3, "Cooking Guide", "food", "article", 4.2);

		Search search = new Search(INDEX_NAME).setRealtime(true);
		SearchResult searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(3, searchResult.getTotalHits());

		// Optimize the index
		zuliaWorkPool.optimizeIndex(INDEX_NAME);

		// Documents should still be searchable after optimize
		search = new Search(INDEX_NAME).setRealtime(true);
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(3, searchResult.getTotalHits());

		// Reindex
		zuliaWorkPool.reindex(INDEX_NAME);

		// Documents should still be searchable after reindex
		search = new Search(INDEX_NAME).setRealtime(true);
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(3, searchResult.getTotalHits());
	}

	@Test
	@Order(8)
	public void conditionalFacetsTest() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// After optimizeAndReindexTest, we have 3 docs

		// Per-facet: category with threshold above totalHits (returned), type with threshold below (skipped)
		Search search = new Search(INDEX_NAME).setAmount(10).setRealtime(true);
		search.addCountFacet(new CountFacet("category").setMaxTotalHitsForFacet(100));
		search.addCountFacet(new CountFacet("type").setMaxTotalHitsForFacet(1));
		SearchResult searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(3, searchResult.getTotalHits());
		Assertions.assertFalse(searchResult.getFacetCounts("category").isEmpty(), "category facet should be returned (threshold 100 >= 3 hits)");
		Assertions.assertNull(searchResult.getFacetCounts("type"), "type facet should be skipped (threshold 1 < 3 hits)");

		// Per-facet: both above threshold → both returned
		search = new Search(INDEX_NAME).setAmount(10).setRealtime(true);
		search.addCountFacet(new CountFacet("category").setMaxTotalHitsForFacet(100));
		search.addCountFacet(new CountFacet("type").setMaxTotalHitsForFacet(100));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertFalse(searchResult.getFacetCounts("category").isEmpty(), "category facet should be returned");
		Assertions.assertFalse(searchResult.getFacetCounts("type").isEmpty(), "type facet should be returned");

		// Per-stat: stat with threshold above (returned), stat with threshold below (skipped)
		search = new Search(INDEX_NAME).setAmount(10).setRealtime(true);
		search.addStat(new NumericStat("rating").setMaxTotalHitsForFacet(100));
		SearchResult resultWithStat = zuliaWorkPool.search(search);
		Assertions.assertFalse(resultWithStat.getStatGroups().isEmpty(), "Stats should be returned (threshold 100 >= 3 hits)");

		search = new Search(INDEX_NAME).setAmount(10).setRealtime(true);
		search.addStat(new NumericStat("rating").setMaxTotalHitsForFacet(1));
		SearchResult resultWithoutStat = zuliaWorkPool.search(search);
		Assertions.assertTrue(resultWithoutStat.getStatGroups().isEmpty(), "Stats should be skipped (threshold 1 < 3 hits)");

		// Default (0) means no limit → facets always returned
		search = new Search(INDEX_NAME).setAmount(10).setRealtime(true);
		search.addCountFacet(new CountFacet("category"));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertFalse(searchResult.getFacetCounts("category").isEmpty(), "Facets should be returned when maxTotalHitsForFacet is 0 (no limit)");

		// Shard-level threshold: maxShardHitsForFacet=1 skips at shard even though maxTotalHitsForFacet=100
		search = new Search(INDEX_NAME).setAmount(10).setRealtime(true);
		search.addCountFacet(new CountFacet("category").setMaxTotalHitsForFacet(100).setMaxShardHitsForFacet(1));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(3, searchResult.getTotalHits());
		Assertions.assertNull(searchResult.getFacetCounts("category"), "Facet should be skipped at shard level (shard threshold 1 < 3 hits)");

		// Shard-level threshold: maxShardHitsForFacet=0 falls back to maxTotalHitsForFacet
		search = new Search(INDEX_NAME).setAmount(10).setRealtime(true);
		search.addCountFacet(new CountFacet("category").setMaxTotalHitsForFacet(100).setMaxShardHitsForFacet(0));
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertFalse(searchResult.getFacetCounts("category").isEmpty(), "Facet should be returned (shard threshold falls back to 100)");
	}

	@Test
	@Order(9)
	public void restart() throws Exception {
		nodeExtension.restartNodes();
	}

	@Test
	@Order(10)
	public void confirm() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// After restart, documents should persist
		Search search = new Search(INDEX_NAME);
		SearchResult searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(3, searchResult.getTotalHits());

		// Field filtering should still work
		Fetch fetch = new Fetch("1", INDEX_NAME);
		fetch.addDocumentField("title");
		FetchResult fetchResult = zuliaWorkPool.fetch(fetch);
		Document doc = fetchResult.getDocument();
		Assertions.assertNotNull(doc.getString("title"));
		Assertions.assertNull(doc.getString("category"));
	}

}
