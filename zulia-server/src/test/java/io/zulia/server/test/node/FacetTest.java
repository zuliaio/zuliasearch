package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.CountFacet;
import io.zulia.client.command.builder.Search;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.SearchResult;
import io.zulia.doc.ResultDocBuilder;
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
	static final NodeExtension nodeExtension = new NodeExtension(1);

	public static final String FACET_TEST_INDEX = "facetTest";

	@Test
	@Order(1)
	public void createIndex() throws Exception {

		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("title");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("stringField1").indexAs(DefaultAnalyzers.STANDARD).facet());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("stringField2").indexAs(DefaultAnalyzers.STANDARD).facet());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("stringField3").indexAs(DefaultAnalyzers.STANDARD).facetWithOwnGroup());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("stringField4").indexAs(DefaultAnalyzers.STANDARD).facet());
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

}
