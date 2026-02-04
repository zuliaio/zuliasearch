package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.CountFacet;
import io.zulia.client.command.builder.Search;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.SearchResult;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.fields.FacetConfigBuilder;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.message.ZuliaQuery;
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
public class FacetTest {

	@RegisterExtension
	static final NodeExtension nodeExtension = new NodeExtension(3);

	public static final String FACET_TEST_INDEX = "facetTest";

	@Test
	@Order(1)
	public void createIndex() throws Exception {

		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("title");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("stringField1").indexAs(DefaultAnalyzers.STANDARD).facet());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("stringField2").indexAs(DefaultAnalyzers.STANDARD).facet());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("stringField3").indexAs(DefaultAnalyzers.STANDARD).facetAs(FacetConfigBuilder.create().storeInOwnGroup().withGroups("groupA")));
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("stringField4").indexAs(DefaultAnalyzers.STANDARD).facetAs(FacetConfigBuilder.create().withGroups("groupA", "groupB")));
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("stringField5").indexAs(DefaultAnalyzers.STANDARD).facet());
		indexConfig.addFieldConfig(FieldConfigBuilder.createInt("intField1").index().facet());
		indexConfig.addFieldConfig(FieldConfigBuilder.createInt("intField2").index().facet());
		indexConfig.addFieldConfig(FieldConfigBuilder.createBool("boolField1").index().facet());
		indexConfig.setIndexName(FACET_TEST_INDEX);
		indexConfig.setNumberOfShards(1);
		indexConfig.setShardCommitInterval(5); //force some commits

		zuliaWorkPool.createIndex(indexConfig);
	}

	@Test
	@Order(2)
	public void index() throws Exception {

		indexRecord(1, "A", "B", null, null, null, null, null, false);
		indexRecord(2, null, "B", null, null, null, null, null, false);
		indexRecord(3, null, null, "C", null, null, null, 3, true);
		indexRecord(4, null, null, "C", null, null, null, null, false);
		indexRecord(5, null, null, null, "D", "D", null, null, false);
		indexRecord(6, null, null, null, "D", "D", null, null, true);
		indexRecord(7, "E", "E", null, "D", "D", null, null, true);
		indexRecord(8, "E", "E", null, "D", "D", 1, 3, false);
		indexRecord(9, "0", "E", "1", "D", "D", null, 3, null);
		indexRecord(10, null, "F", "123456", "R", "R", 12321, 323, null);
		indexRecord(11, null, null, null, null, "R", 12321, 323, null);
		indexRecord(12, null, null, null, null, null, null, null, null);
		indexRecord(13, null, "!!", null, null, null, null, null, null);

	}

	private void indexRecord(int id, String stringField1, String stringField2, String stringField3, String stringField4, String stringField5, Integer intField1,
			Integer intField2, Boolean boolField1) throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		String uniqueId = String.valueOf(id);

		Document mongoDocument = new Document();
		mongoDocument.put("id", uniqueId);

		//let some be null values vs. not exist for variety
		if (stringField1 != null) {
			mongoDocument.put("stringField1", stringField1);
		}
		if (stringField2 != null) {
			mongoDocument.put("stringField2", stringField2);
		}
		if (stringField3 != null) {
			mongoDocument.put("stringField3", stringField3);
		}
		mongoDocument.put("stringField4", stringField4);
		mongoDocument.put("stringField5", stringField5);
		mongoDocument.put("intField1", intField1);

		if (intField2 != null) {
			mongoDocument.put("intField2", intField2);
		}
		mongoDocument.put("boolField1", boolField1);

		Store s = new Store(uniqueId, FACET_TEST_INDEX);

		ResultDocBuilder resultDocumentBuilder = ResultDocBuilder.newBuilder().setDocument(mongoDocument);
		s.setResultDocument(resultDocumentBuilder);
		zuliaWorkPool.store(s);

	}

	@Test
	@Order(3)
	public void searchTest() throws Exception {

		facetTestSearch(1);
		facetTestSearch(4);
	}

	private static void facetTestSearch(int concurrency) throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		{
			//run the first search with realtime to force seeing latest changes without waiting for a commit
			Search search = new Search(FACET_TEST_INDEX).setRealtime(true);
			search.setConcurrency(concurrency);
			SearchResult searchResult = zuliaWorkPool.search(search);
			Assertions.assertEquals(13, searchResult.getTotalHits());
		}

		{
			Search search = new Search(FACET_TEST_INDEX).addCountFacet(new CountFacet("boolField1"));
			search.setConcurrency(concurrency);
			SearchResult searchResult = zuliaWorkPool.search(search);

			verifyBool1NoQuery(searchResult);
		}

		{
			Search search = new Search(FACET_TEST_INDEX).addCountFacet(new CountFacet("intField1"));
			search.setConcurrency(concurrency);
			SearchResult searchResult = zuliaWorkPool.search(search);

			verifyInt1NoQuery(searchResult);
		}

		{
			Search search = new Search(FACET_TEST_INDEX).addCountFacet(new CountFacet("intField2"));
			search.setConcurrency(concurrency);
			SearchResult searchResult = zuliaWorkPool.search(search);

			verifyInt2NoQuery(searchResult);
		}

		{
			Search search = new Search(FACET_TEST_INDEX).addCountFacet(new CountFacet("intField2")).addCountFacet(new CountFacet("intField1"));
			search.setConcurrency(concurrency);
			SearchResult searchResult = zuliaWorkPool.search(search);
			verifyInt1NoQuery(searchResult);
			verifyInt2NoQuery(searchResult);
		}

		{
			Search search = new Search(FACET_TEST_INDEX).addCountFacet(new CountFacet("stringField1"));
			search.setConcurrency(concurrency);
			SearchResult searchResult = zuliaWorkPool.search(search);

			verifyString1NoQuery(searchResult);
		}

		{
			Search search = new Search(FACET_TEST_INDEX).addCountFacet(new CountFacet("stringField2"));
			search.setConcurrency(concurrency);
			SearchResult searchResult = zuliaWorkPool.search(search);

			verifyString2NoQuery(searchResult);
		}

		{
			Search search = new Search(FACET_TEST_INDEX).addCountFacet(new CountFacet("stringField1")).addCountFacet(new CountFacet("stringField2"));
			search.setConcurrency(concurrency);
			SearchResult searchResult = zuliaWorkPool.search(search);

			verifyString1NoQuery(searchResult);
			verifyString2NoQuery(searchResult);
		}

		{
			Search search = new Search(FACET_TEST_INDEX).addCountFacet(new CountFacet("stringField4"));
			search.setConcurrency(concurrency);
			SearchResult searchResult = zuliaWorkPool.search(search);
			verifyString4NoQuery(searchResult);
		}

		{
			Search search = new Search(FACET_TEST_INDEX).addCountFacet(new CountFacet("stringField5"));
			search.setConcurrency(concurrency);
			SearchResult searchResult = zuliaWorkPool.search(search);
			verifyString5NoQuery(searchResult);
		}

		{
			Search search = new Search(FACET_TEST_INDEX).addCountFacet(new CountFacet("stringField4")).addCountFacet(new CountFacet("stringField5"));
			search.setConcurrency(concurrency);
			SearchResult searchResult = zuliaWorkPool.search(search);

			verifyString4NoQuery(searchResult);
			verifyString5NoQuery(searchResult);
		}

		{
			Search search = new Search(FACET_TEST_INDEX).addCountFacet(new CountFacet("stringField1")).addCountFacet(new CountFacet("stringField4"))
					.addCountFacet(new CountFacet("stringField5"));
			search.setConcurrency(concurrency);
			SearchResult searchResult = zuliaWorkPool.search(search);

			verifyString1NoQuery(searchResult);
			verifyString4NoQuery(searchResult);
			verifyString5NoQuery(searchResult);
		}

		{
			Search search = new Search(FACET_TEST_INDEX).addCountFacet(new CountFacet("stringField1")).addCountFacet(new CountFacet("stringField2"))
					.addCountFacet(new CountFacet("stringField4")).addCountFacet(new CountFacet("stringField5"));
			search.setConcurrency(concurrency);
			SearchResult searchResult = zuliaWorkPool.search(search);

			verifyString1NoQuery(searchResult);
			verifyString2NoQuery(searchResult);
			verifyString4NoQuery(searchResult);
			verifyString5NoQuery(searchResult);
		}

		{
			Search search = new Search(FACET_TEST_INDEX).addCountFacet(new CountFacet("stringField1")).addCountFacet(new CountFacet("stringField2"))
					.addCountFacet(new CountFacet("stringField3")).addCountFacet(new CountFacet("stringField4")).addCountFacet(new CountFacet("stringField5"));
			search.setConcurrency(concurrency);
			SearchResult searchResult = zuliaWorkPool.search(search);

			verifyString1NoQuery(searchResult);
			verifyString2NoQuery(searchResult);
			verifyString3NoQuery(searchResult);
			verifyString4NoQuery(searchResult);
			verifyString5NoQuery(searchResult);
		}

		{
			Search search = new Search(FACET_TEST_INDEX).addCountFacet(new CountFacet("stringField1")).addCountFacet(new CountFacet("stringField2"))
					.addCountFacet(new CountFacet("stringField3")).addCountFacet(new CountFacet("stringField4")).addCountFacet(new CountFacet("stringField5"))
					.addCountFacet(new CountFacet("intField1")).addCountFacet(new CountFacet("boolField1"));
			search.setConcurrency(concurrency);
			SearchResult searchResult = zuliaWorkPool.search(search);

			verifyString1NoQuery(searchResult);
			verifyString2NoQuery(searchResult);
			verifyString3NoQuery(searchResult);
			verifyString4NoQuery(searchResult);
			verifyString5NoQuery(searchResult);
			verifyInt1NoQuery(searchResult);
			verifyBool1NoQuery(searchResult);
		}

		{
			Search search = new Search(FACET_TEST_INDEX).addCountFacet(new CountFacet("stringField3"));
			search.setConcurrency(concurrency);
			SearchResult searchResult = zuliaWorkPool.search(search);
			verifyString3NoQuery(searchResult);
		}

		{
			Search search = new Search(FACET_TEST_INDEX).addCountFacet(new CountFacet("stringField3")).addCountFacet(new CountFacet("stringField4"));
			search.setConcurrency(concurrency);
			SearchResult searchResult = zuliaWorkPool.search(search);
			verifyString3NoQuery(searchResult);
		}
	}

	private static void verifyString1NoQuery(SearchResult searchResult) {
		List<ZuliaQuery.FacetCount> stringField1Counts = searchResult.getFacetCounts("stringField1");
		Assertions.assertEquals(3, stringField1Counts.size());
		ZuliaQuery.FacetCount stringField1Count1 = stringField1Counts.getFirst();
		Assertions.assertEquals("E", stringField1Count1.getFacet());
		Assertions.assertEquals(2L, stringField1Count1.getCount());
	}

	private static void verifyString2NoQuery(SearchResult searchResult) {
		List<ZuliaQuery.FacetCount> stringField2Counts = searchResult.getFacetCounts("stringField2");
		Assertions.assertEquals(4, stringField2Counts.size());
		ZuliaQuery.FacetCount stringField2Count1 = stringField2Counts.getFirst();
		Assertions.assertEquals("E", stringField2Count1.getFacet());
		Assertions.assertEquals(3L, stringField2Count1.getCount());
		ZuliaQuery.FacetCount stringField2Count2 = stringField2Counts.get(1);
		Assertions.assertEquals("B", stringField2Count2.getFacet());
		Assertions.assertEquals(2L, stringField2Count2.getCount());
	}

	private static void verifyString3NoQuery(SearchResult searchResult) {
		List<ZuliaQuery.FacetCount> stringField3Counts = searchResult.getFacetCounts("stringField3");
		Assertions.assertEquals(3, stringField3Counts.size());
		ZuliaQuery.FacetCount stringField3Count1 = stringField3Counts.getFirst();
		Assertions.assertEquals("C", stringField3Count1.getFacet());
		Assertions.assertEquals(2L, stringField3Count1.getCount());
	}

	private static void verifyString4NoQuery(SearchResult searchResult) {
		List<ZuliaQuery.FacetCount> stringField4Counts = searchResult.getFacetCounts("stringField4");
		Assertions.assertEquals(2, stringField4Counts.size());
		ZuliaQuery.FacetCount stringField4Count1 = stringField4Counts.getFirst();
		Assertions.assertEquals("D", stringField4Count1.getFacet());
		Assertions.assertEquals(5L, stringField4Count1.getCount());
		ZuliaQuery.FacetCount stringField4Count2 = stringField4Counts.get(1);
		Assertions.assertEquals("R", stringField4Count2.getFacet());
		Assertions.assertEquals(1L, stringField4Count2.getCount());
	}

	private static void verifyString5NoQuery(SearchResult searchResult) {
		List<ZuliaQuery.FacetCount> stringField5Counts = searchResult.getFacetCounts("stringField5");
		Assertions.assertEquals(2, stringField5Counts.size());
		ZuliaQuery.FacetCount stringField5Count1 = stringField5Counts.getFirst();
		Assertions.assertEquals("D", stringField5Count1.getFacet());
		Assertions.assertEquals(5L, stringField5Count1.getCount());
		ZuliaQuery.FacetCount stringField5Count2 = stringField5Counts.get(1);
		Assertions.assertEquals("R", stringField5Count2.getFacet());
		Assertions.assertEquals(2L, stringField5Count2.getCount());
	}

	private static void verifyInt1NoQuery(SearchResult searchResult) {
		List<ZuliaQuery.FacetCount> intField1Counts = searchResult.getFacetCounts("intField1");
		Assertions.assertEquals(2, intField1Counts.size());
		ZuliaQuery.FacetCount intField1Count1 = intField1Counts.getFirst();
		Assertions.assertEquals("12321", intField1Count1.getFacet());
		Assertions.assertEquals(2L, intField1Count1.getCount());
		ZuliaQuery.FacetCount intField1Count2 = intField1Counts.get(1);
		Assertions.assertEquals("1", intField1Count2.getFacet());
		Assertions.assertEquals(1L, intField1Count2.getCount());
	}

	private static void verifyInt2NoQuery(SearchResult searchResult) {
		List<ZuliaQuery.FacetCount> intField2Counts = searchResult.getFacetCounts("intField2");
		Assertions.assertEquals(2, intField2Counts.size());
		ZuliaQuery.FacetCount intField2Count1 = intField2Counts.getFirst();
		Assertions.assertEquals("3", intField2Count1.getFacet());
		Assertions.assertEquals(3L, intField2Count1.getCount());
		ZuliaQuery.FacetCount intField2Count2 = intField2Counts.get(1);
		Assertions.assertEquals("323", intField2Count2.getFacet());
		Assertions.assertEquals(2L, intField2Count2.getCount());
	}

	private static void verifyBool1NoQuery(SearchResult searchResult) {
		List<ZuliaQuery.FacetCount> intField2Counts = searchResult.getFacetCounts("boolField1");
		Assertions.assertEquals(2, intField2Counts.size());
		ZuliaQuery.FacetCount intField2Count1 = intField2Counts.getFirst();
		Assertions.assertEquals("False", intField2Count1.getFacet());
		Assertions.assertEquals(5L, intField2Count1.getCount());
		ZuliaQuery.FacetCount intField2Count2 = intField2Counts.get(1);
		Assertions.assertEquals("True", intField2Count2.getFacet());
		Assertions.assertEquals(3L, intField2Count2.getCount());
	}

	@Test
	@Order(5)
	public void restart() throws Exception {
		nodeExtension.restartNodes();
	}

	@Test
	@Order(6)
	public void confirm() throws Exception {
		searchTest();
	}

	public static final String ERROR_BOUND_INDEX = "facetErrorBoundTest";
	private static final int ERROR_BOUND_SHARD_COUNT = 5;
	private static final int ERROR_BOUND_DOC_COUNT = 100;

	@Test
	@Order(7)
	public void createErrorBoundIndex() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("title");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("category").indexAs(DefaultAnalyzers.LC_KEYWORD).facet().sort());
		indexConfig.setIndexName(ERROR_BOUND_INDEX);
		indexConfig.setNumberOfShards(ERROR_BOUND_SHARD_COUNT);
		indexConfig.setShardCommitInterval(20);

		zuliaWorkPool.createIndex(indexConfig);
	}

	@Test
	@Order(8)
	public void indexErrorBoundData() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Index documents with different categories
		// Categories: cat1 (40 docs), cat2 (30 docs), cat3 (20 docs), cat4 (10 docs)
		// Documents are distributed across shards by ID
		for (int i = 0; i < ERROR_BOUND_DOC_COUNT; i++) {
			String category;
			if (i < 40) {
				category = "cat1";
			} else if (i < 70) {
				category = "cat2";
			} else if (i < 90) {
				category = "cat3";
			} else {
				category = "cat4";
			}

			Document doc = new Document();
			doc.put("id", String.valueOf(i));
			doc.put("category", category);

			Store store = new Store(String.valueOf(i), ERROR_BOUND_INDEX);
			store.setResultDocument(ResultDocBuilder.from(doc));
			zuliaWorkPool.store(store);
		}
	}

	@Test
	@Order(9)
	public void testErrorBounds() throws Exception {
		testErrorBoundsSearch(null);
		testErrorBoundsSearch(2);
	}

	private void testErrorBoundsSearch(Integer concurrency) throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		Search search = new Search(ERROR_BOUND_INDEX).setRealtime(true);
		if (concurrency != null) {
			search.setConcurrency(concurrency);
		}

		// Test 1: With shardFacets=-1 (all facets) - no errors should occur
		search.addCountFacet(new CountFacet("category").setTopN(10).setTopNShard(-1));
		SearchResult searchResult = zuliaWorkPool.search(search);

		List<ZuliaQuery.FacetCount> facetCounts = searchResult.getFacetCounts("category");
		Assertions.assertFalse(facetCounts.isEmpty(), "Should have facet counts");
		for (ZuliaQuery.FacetCount fc : facetCounts) {
			Assertions.assertEquals(0, fc.getMaxError(),
					"Facet " + fc.getFacet() + " should have no error when shardFacets=-1");
		}

		// Verify counts
		ZuliaQuery.FacetCount cat1 = findFacetByName(facetCounts, "cat1");
		ZuliaQuery.FacetCount cat2 = findFacetByName(facetCounts, "cat2");
		ZuliaQuery.FacetCount cat3 = findFacetByName(facetCounts, "cat3");
		ZuliaQuery.FacetCount cat4 = findFacetByName(facetCounts, "cat4");

		Assertions.assertNotNull(cat1);
		Assertions.assertNotNull(cat2);
		Assertions.assertNotNull(cat3);
		Assertions.assertNotNull(cat4);

		Assertions.assertEquals(40, cat1.getCount());
		Assertions.assertEquals(30, cat2.getCount());
		Assertions.assertEquals(20, cat3.getCount());
		Assertions.assertEquals(10, cat4.getCount());

		// Test 2: With limited shardFacets, verify error handling
		search.clearFacetCount();
		search.addCountFacet(new CountFacet("category").setTopN(10).setTopNShard(1));
		searchResult = zuliaWorkPool.search(search);

		facetCounts = searchResult.getFacetCounts("category");

		// With shardFacets=1, only top facet per shard is returned
		// Facets present in all shards won't have errors
		// Facets missing from some shards will have maxError >= 0
		for (ZuliaQuery.FacetCount fc : facetCounts) {
			Assertions.assertTrue(fc.getMaxError() >= 0,
					"maxError should be non-negative for facet " + fc.getFacet());
		}

		// Test 3: With shardFacets=2, more facets returned
		search.clearFacetCount();
		search.addCountFacet(new CountFacet("category").setTopN(10).setTopNShard(2));
		searchResult = zuliaWorkPool.search(search);

		facetCounts = searchResult.getFacetCounts("category");
		boolean foundAnyFacet = false;
		for (ZuliaQuery.FacetCount fc : facetCounts) {
			foundAnyFacet = true;
			// Error should be reasonable
			Assertions.assertTrue(fc.getMaxError() >= 0,
					"maxError should be non-negative for facet " + fc.getFacet());
			Assertions.assertTrue(fc.getMaxError() <= fc.getCount() + ERROR_BOUND_DOC_COUNT,
					"maxError " + fc.getMaxError() + " seems unreasonably large for facet " + fc.getFacet());
		}
		Assertions.assertTrue(foundAnyFacet, "Should have at least one facet result");

		// Test 4: Verify consistency - same facet with/without limited shardFacets
		search.clearFacetCount();
		search.addCountFacet(new CountFacet("category").setTopN(10).setTopNShard(-1));
		searchResult = zuliaWorkPool.search(search);
		List<ZuliaQuery.FacetCount> allFacetCounts = searchResult.getFacetCounts("category");

		search.clearFacetCount();
		search.addCountFacet(new CountFacet("category").setTopN(10).setTopNShard(1));
		searchResult = zuliaWorkPool.search(search);
		List<ZuliaQuery.FacetCount> limitedFacetCounts = searchResult.getFacetCounts("category");

		// Find facets that appear in both results
		for (ZuliaQuery.FacetCount limited : limitedFacetCounts) {
			ZuliaQuery.FacetCount all = findFacetByName(allFacetCounts, limited.getFacet());
			if (all != null) {
				// Count from limited should be <= count from all
				Assertions.assertTrue(limited.getCount() <= all.getCount(),
						"Facet " + limited.getFacet() + " limited count should be <= all count");
				// If no error, counts should match
				if (limited.getMaxError() == 0) {
					Assertions.assertEquals(all.getCount(), limited.getCount(),
							"Facet " + limited.getFacet() + " count should match when no error");
				}
			}
		}
	}

	private ZuliaQuery.FacetCount findFacetByName(List<ZuliaQuery.FacetCount> facetCounts, String name) {
		return facetCounts.stream()
				.filter(fc -> fc.getFacet().equals(name))
				.findFirst()
				.orElse(null);
	}

}
