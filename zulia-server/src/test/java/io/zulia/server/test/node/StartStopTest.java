package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.ZuliaConstants;
import io.zulia.client.command.Query;
import io.zulia.client.command.Reindex;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.CountFacet;
import io.zulia.client.command.builder.FilterQuery;
import io.zulia.client.command.builder.MatchAllQuery;
import io.zulia.client.command.builder.ScoredQuery;
import io.zulia.client.command.builder.Search;
import io.zulia.client.command.builder.Sort;
import io.zulia.client.command.builder.TermQuery;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.QueryResult;
import io.zulia.client.result.SearchResult;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.message.ZuliaIndex.FacetAs.DateHandling;
import io.zulia.message.ZuliaIndex.FieldConfig.FieldType;
import io.zulia.message.ZuliaQuery;
import io.zulia.message.ZuliaQuery.FacetCount;
import io.zulia.util.ZuliaUtil;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import static io.zulia.message.ZuliaQuery.FieldSort.Direction.ASCENDING;
import static io.zulia.message.ZuliaQuery.FieldSort.Direction.DESCENDING;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class StartStopTest {

	public static final String FACET_TEST_INDEX = "plugged-54a725bc148f6dd7d62bc600";

	private final int COUNT_PER_ISSN = 10;
	private final String uniqueIdPrefix = "myId-";

	private final String[] issns = new String[] { "1234-1234", "3333-1234", "1234-5555", "1234-4444", "2222-2222", "3331-3333" };
	private final String[] eissns = new String[] { "3234-1234", "4333-1234", "5234-5555", "6234-4444", "9222-2222", "3431-3638" };

	private int totalRecords = COUNT_PER_ISSN * issns.length;
	private int totalRecordsWithTitle = COUNT_PER_ISSN * (issns.length - 1);

	private static ZuliaWorkPool zuliaWorkPool;

	@BeforeAll
	public static void initAll() throws Exception {

		TestHelper.createNodes(3);

		TestHelper.startNodes();

		Thread.sleep(2000);

		zuliaWorkPool = TestHelper.createClient();

		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("title");
		indexConfig.addFieldConfig(FieldConfigBuilder.create("id", FieldType.STRING).indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.create("title", FieldType.STRING).indexAs(DefaultAnalyzers.STANDARD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.create("issn", FieldType.STRING).indexAs(DefaultAnalyzers.LC_KEYWORD).facet().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.create("eissn", FieldType.STRING).indexAs(DefaultAnalyzers.LC_KEYWORD));
		indexConfig.addFieldConfig(FieldConfigBuilder.create("uid", FieldType.STRING).indexAs(DefaultAnalyzers.LC_KEYWORD));
		indexConfig.addFieldConfig(FieldConfigBuilder.create("an", FieldType.NUMERIC_INT).index().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.create("country", FieldType.STRING).indexAs(DefaultAnalyzers.LC_KEYWORD).facet());
		indexConfig.addFieldConfig(FieldConfigBuilder.create("date", FieldType.DATE).index().facetAs(DateHandling.DATE_YYYY_MM_DD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.create("testList", FieldType.STRING).index());
		indexConfig.addFieldConfig(FieldConfigBuilder.create("testBool", FieldType.BOOL).index());
		indexConfig.setIndexName(FACET_TEST_INDEX);
		indexConfig.setNumberOfShards(1);
		indexConfig.setShardCommitInterval(20); //force some commits

		zuliaWorkPool.createIndex(indexConfig);
	}

	@Test
	@Order(2)
	public void index() throws Exception {
		int id = 0;
		{
			for (int j = 0; j < issns.length; j++) {
				String issn = issns[j];
				String eissn = eissns[j];

				for (int i = 0; i < COUNT_PER_ISSN; i++) {
					indexRecord(id, issn, eissn, i);
					id++;
				}

				//overwrite a few records with the same values
				for (int i = 1; i <= 2; i++) {
					indexRecord(id - i, issn, eissn, i);
				}

			}
		}

	}

	private void indexRecord(int id, String issn, String eissn, int i) throws Exception {
		boolean half = (i % 2 == 0);
		boolean tenth = (i % 10 == 0);

		String uniqueId = uniqueIdPrefix + id;

		Document mongoDocument = new Document();
		mongoDocument.put("issn", issn);
		mongoDocument.put("eissn", eissn);

		mongoDocument.put("id", id);
		if (id != 0) {
			mongoDocument.put("an", id);
		}

		if (id == 0) {
			mongoDocument.put("testBool", true);
		}
		else if (id == 1) {
			mongoDocument.put("testBool", 1);
		}
		else if (id == 2) {
			mongoDocument.put("testBool", "Yes");
		}
		else if (id == 3) {
			mongoDocument.put("testBool", "true");
		}
		else if (id == 4) {
			mongoDocument.put("testBool", "Y");
		}
		else if (id == 5) {
			mongoDocument.put("testBool", "T");
		}
		else if (id == 6) {
			mongoDocument.put("testBool", false);
		}
		else if (id == 7) {
			mongoDocument.put("testBool", "False");
		}
		else if (id == 8) {
			mongoDocument.put("testBool", "F");
		}
		else if (id == 9) {
			mongoDocument.put("testBool", 0);
		}

		if (!issn.equals("3331-3333")) {
			if (half) {
				mongoDocument.put("title", "Facet Userguide");
			}
			else {
				mongoDocument.put("title", "Special Userguide");
			}
		}

		if (half) { // 1/2 of input
			mongoDocument.put("country", "US");
			mongoDocument.put("testList", Arrays.asList("one", "two"));

		}
		else { // 1/2 of input
			mongoDocument.put("country", "France");
			mongoDocument.put("testList", Arrays.asList("a", "b", "c"));
		}

		if (tenth) { // 1/10 of input

			Date d = Date.from(LocalDate.of(2014, Month.OCTOBER, 4).atStartOfDay(ZoneId.of("UTC")).toInstant());
			mongoDocument.put("date", d);
		}
		else if (half) { // 2/5 of input
			Date d = Date.from(LocalDate.of(2013, Month.SEPTEMBER, 4).atStartOfDay(ZoneId.of("UTC")).toInstant());
			mongoDocument.put("date", d);
		}
		else { // 1/2 of input
			Date d = Date.from(LocalDate.of(2012, 8, 4).atStartOfDay(ZoneId.of("UTC")).toInstant());
			mongoDocument.put("date", d);
		}

		Store s = new Store(uniqueId, FACET_TEST_INDEX);

		ResultDocBuilder resultDocumentBuilder = ResultDocBuilder.newBuilder().setDocument(mongoDocument);
		if (half) {
			resultDocumentBuilder.setMetadata(new Document("test", "someValue"));
		}

		s.setResultDocument(resultDocumentBuilder);

		zuliaWorkPool.store(s);

	}

	@Test
	@Order(3)
	public void sortScore() throws Exception {
		Query q = new Query(FACET_TEST_INDEX, "issn:\"1234-1234\" OR country:US", 10);
		q.addFieldSort(ZuliaConstants.SCORE_FIELD, ASCENDING);
		QueryResult queryResult = zuliaWorkPool.query(q);

		double lowScore = -1;
		double highScore = -1;
		for (ZuliaQuery.ScoredResult result : queryResult.getResults()) {
			Assertions.assertTrue(result.getScore() > 0);
			if (lowScore < 0 || result.getScore() < lowScore) {
				lowScore = result.getScore();
			}
		}

		q = new Query(FACET_TEST_INDEX, "issn:\"1234-1234\" OR country:US", 10);
		q.addFieldSort(ZuliaConstants.SCORE_FIELD, DESCENDING);
		queryResult = zuliaWorkPool.query(q);

		for (ZuliaQuery.ScoredResult result : queryResult.getResults()) {
			Assertions.assertTrue(result.getScore() > 0);
			if (highScore < 0 || result.getScore() > highScore) {
				highScore = result.getScore();
			}
		}

		Assertions.assertTrue(highScore > lowScore);

	}

	@Test
	@Order(3)
	public void sortScoreBuilder() throws Exception {

		Search search = new Search(FACET_TEST_INDEX).setAmount(10);
		search.addQuery(new ScoredQuery("issn:\"1234-1234\" OR country:US"));
		search.addSort(new Sort(ZuliaConstants.SCORE_FIELD).ascending());

		SearchResult searchResult = zuliaWorkPool.search(search);

		double lowScore = -1;
		double highScore = -1;
		for (ZuliaQuery.ScoredResult result : searchResult.getResults()) {
			Assertions.assertTrue(result.getScore() > 0);
			if (lowScore < 0 || result.getScore() < lowScore) {
				lowScore = result.getScore();
			}
		}

		search.clearSort();
		search.addSort(new Sort(ZuliaConstants.SCORE_FIELD).descending());
		searchResult = zuliaWorkPool.search(search);

		for (ZuliaQuery.ScoredResult result : searchResult.getResults()) {
			Assertions.assertTrue(result.getScore() > 0);
			if (highScore < 0 || result.getScore() > highScore) {
				highScore = result.getScore();
			}
		}

		Assertions.assertTrue(highScore > lowScore);

	}

	@Test
	@Order(3)
	public void lengthTest() throws Exception {

		Query q = new Query(FACET_TEST_INDEX, null, 0);
		QueryResult queryResult = zuliaWorkPool.query(q);
		long total = queryResult.getTotalHits();

		q = new Query(FACET_TEST_INDEX, "|country|:2", 0);
		queryResult = zuliaWorkPool.query(q);
		Assertions.assertEquals(total / 2, queryResult.getTotalHits());

		q = new Query(FACET_TEST_INDEX, "|country|:[0 TO 1]", 0);
		queryResult = zuliaWorkPool.query(q);
		Assertions.assertEquals(0, queryResult.getTotalHits());

		q = new Query(FACET_TEST_INDEX, "|||testList|||:3", 0);
		queryResult = zuliaWorkPool.query(q);
		Assertions.assertEquals(total / 2, queryResult.getTotalHits());

		q = new Query(FACET_TEST_INDEX, "|||testList|||:[2 TO 3]", 0);
		queryResult = zuliaWorkPool.query(q);
		Assertions.assertEquals(total, queryResult.getTotalHits());

		q = new Query(FACET_TEST_INDEX, null, (int) total / 2).addFieldSort("|country|");
		queryResult = zuliaWorkPool.query(q);
		for (Document document : queryResult.getDocuments()) {
			Assertions.assertEquals(document.getString("country"), "US");
		}

		q = new Query(FACET_TEST_INDEX, null, (int) total / 2).addFieldSort("|country|", DESCENDING);
		queryResult = zuliaWorkPool.query(q);
		for (Document document : queryResult.getDocuments()) {
			Assertions.assertEquals(document.getString("country"), "France");
		}

	}

	@Test
	@Order(3)
	public void lengthTestBuilder() throws Exception {

		Search s = new Search(FACET_TEST_INDEX);
		SearchResult searchResult = zuliaWorkPool.search(s);
		long total = searchResult.getTotalHits();

		s = new Search(FACET_TEST_INDEX).addQuery(new FilterQuery("|country|:2"));
		searchResult = zuliaWorkPool.search(s);
		Assertions.assertEquals(total / 2, searchResult.getTotalHits());

		s = new Search(FACET_TEST_INDEX).addQuery(new FilterQuery("|country|:[0 TO 1]"));
		searchResult = zuliaWorkPool.search(s);
		Assertions.assertEquals(0, searchResult.getTotalHits());

		s = new Search(FACET_TEST_INDEX).addQuery(new FilterQuery("|||testList|||:3"));
		searchResult = zuliaWorkPool.search(s);
		Assertions.assertEquals(total / 2, searchResult.getTotalHits());

		s = new Search(FACET_TEST_INDEX).addQuery(new FilterQuery("|||testList|||:[2 TO 3]"));
		searchResult = zuliaWorkPool.search(s);
		Assertions.assertEquals(total, searchResult.getTotalHits());

		s = new Search(FACET_TEST_INDEX).setAmount((int) total / 2).addSort(new Sort("|country|"));
		searchResult = zuliaWorkPool.search(s);
		for (Document document : searchResult.getDocuments()) {
			Assertions.assertEquals(document.getString("country"), "US");
		}

		s = new Search(FACET_TEST_INDEX).setAmount((int) total / 2).addSort(new Sort("|country|").descending());
		searchResult = zuliaWorkPool.search(s);
		for (Document document : searchResult.getDocuments()) {
			Assertions.assertEquals(document.getString("country"), "France");
		}

	}

	@Test
	@Order(3)
	public void boolTest() throws Exception {
		Query q = new Query(FACET_TEST_INDEX, "testBool:true", 0);
		QueryResult queryResult = zuliaWorkPool.query(q);
		Assertions.assertEquals(6, queryResult.getTotalHits());

		q = new Query(FACET_TEST_INDEX, "testBool:1", 0);
		queryResult = zuliaWorkPool.query(q);
		Assertions.assertEquals(6, queryResult.getTotalHits());

		q = new Query(FACET_TEST_INDEX, "testBool:True", 0);
		queryResult = zuliaWorkPool.query(q);
		Assertions.assertEquals(6, queryResult.getTotalHits());

		q = new Query(FACET_TEST_INDEX, "testBool:y", 0);
		queryResult = zuliaWorkPool.query(q);
		Assertions.assertEquals(6, queryResult.getTotalHits());

		q = new Query(FACET_TEST_INDEX, "testBool:yes", 0);
		queryResult = zuliaWorkPool.query(q);
		Assertions.assertEquals(6, queryResult.getTotalHits());

		q = new Query(FACET_TEST_INDEX, "testBool:false", 0);
		queryResult = zuliaWorkPool.query(q);
		Assertions.assertEquals(4, queryResult.getTotalHits());

		q = new Query(FACET_TEST_INDEX, "testBool:0", 0);
		queryResult = zuliaWorkPool.query(q);
		Assertions.assertEquals(4, queryResult.getTotalHits());

		q = new Query(FACET_TEST_INDEX, "testBool:no", 0);
		queryResult = zuliaWorkPool.query(q);
		Assertions.assertEquals(4, queryResult.getTotalHits());

		q = new Query(FACET_TEST_INDEX, "testBool:f", 0);
		queryResult = zuliaWorkPool.query(q);
		Assertions.assertEquals(4, queryResult.getTotalHits());

		q = new Query(FACET_TEST_INDEX, "testBool:fake", 0);
		queryResult = zuliaWorkPool.query(q);
		Assertions.assertEquals(0, queryResult.getTotalHits());

		q = new Query(FACET_TEST_INDEX, "testBool:*", 0);
		queryResult = zuliaWorkPool.query(q);
		Assertions.assertEquals(10, queryResult.getTotalHits());
	}

	@Test
	@Order(3)
	public void boolTestBuilder() throws Exception {
		Search s = new Search(FACET_TEST_INDEX).addQuery(new FilterQuery("testBool:true"));
		SearchResult searchResult = zuliaWorkPool.search(s);
		Assertions.assertEquals(6, searchResult.getTotalHits());

		s = new Search(FACET_TEST_INDEX).addQuery(new FilterQuery("testBool:1"));
		searchResult = zuliaWorkPool.search(s);
		Assertions.assertEquals(6, searchResult.getTotalHits());

		s = new Search(FACET_TEST_INDEX).addQuery(new FilterQuery("testBool:True"));
		searchResult = zuliaWorkPool.search(s);
		Assertions.assertEquals(6, searchResult.getTotalHits());

		s = new Search(FACET_TEST_INDEX).addQuery(new FilterQuery("testBool:y"));
		searchResult = zuliaWorkPool.search(s);
		Assertions.assertEquals(6, searchResult.getTotalHits());

		s = new Search(FACET_TEST_INDEX).addQuery(new FilterQuery("testBool:yes"));
		searchResult = zuliaWorkPool.search(s);
		Assertions.assertEquals(6, searchResult.getTotalHits());

		s = new Search(FACET_TEST_INDEX).addQuery(new FilterQuery("testBool:false"));
		searchResult = zuliaWorkPool.search(s);
		Assertions.assertEquals(4, searchResult.getTotalHits());

		s = new Search(FACET_TEST_INDEX).addQuery(new FilterQuery("testBool:0"));
		searchResult = zuliaWorkPool.search(s);
		Assertions.assertEquals(4, searchResult.getTotalHits());

		s = new Search(FACET_TEST_INDEX).addQuery(new FilterQuery("testBool:no"));
		searchResult = zuliaWorkPool.search(s);
		Assertions.assertEquals(4, searchResult.getTotalHits());

		s = new Search(FACET_TEST_INDEX).addQuery(new FilterQuery("testBool:f"));
		searchResult = zuliaWorkPool.search(s);
		Assertions.assertEquals(4, searchResult.getTotalHits());

		s = new Search(FACET_TEST_INDEX).addQuery(new FilterQuery("testBool:fake"));
		searchResult = zuliaWorkPool.search(s);
		Assertions.assertEquals(0, searchResult.getTotalHits());

		s = new Search(FACET_TEST_INDEX).addQuery(new FilterQuery("testBool:*"));
		searchResult = zuliaWorkPool.search(s);
		Assertions.assertEquals(10, searchResult.getTotalHits());
	}

	@Test
	@Order(3)
	public void termTest() throws Exception {
		Query q = new Query(FACET_TEST_INDEX, null, 0);
		q.addTermQuery(Arrays.asList("1", "2", "3", "4"), "id");
		QueryResult queryResult = zuliaWorkPool.query(q);
		Assertions.assertEquals(4, queryResult.getTotalHits());

		q = new Query(FACET_TEST_INDEX, null, 0);
		q.addTermQuery(Arrays.asList("1", "2", "3", "4"), "id");
		q.addFilterQuery("testBool:true");
		queryResult = zuliaWorkPool.query(q);
		Assertions.assertEquals(4, queryResult.getTotalHits());

		q = new Query(FACET_TEST_INDEX, null, 0);
		q.addTermQuery(Arrays.asList("1", "2", "3", "4"), "id");
		q.addFilterQuery("country:US");
		queryResult = zuliaWorkPool.query(q);
		Assertions.assertEquals(2, queryResult.getTotalHits());
	}

	@Test
	@Order(3)
	public void termTestBuilder() throws Exception {
		Search s = new Search(FACET_TEST_INDEX);
		s.addQuery(new TermQuery("id").addTerms("1", "2", "3", "4"));
		SearchResult searchResult = zuliaWorkPool.search(s);
		Assertions.assertEquals(4, searchResult.getTotalHits());

		s = new Search(FACET_TEST_INDEX);
		s.addQuery(new TermQuery("id").addTerms(Arrays.asList("1", "2", "3", "4")));
		s.addQuery(new FilterQuery("testBool:true"));
		searchResult = zuliaWorkPool.search(s);
		Assertions.assertEquals(4, searchResult.getTotalHits());

		s = new Search(FACET_TEST_INDEX);
		s.addQuery(new TermQuery("id").addTerm("1").addTerm("2").addTerm("3").addTerm("4"));
		s.addQuery(new FilterQuery("country:US"));
		searchResult = zuliaWorkPool.search(s);
		Assertions.assertEquals(2, searchResult.getTotalHits());

		s = new Search(FACET_TEST_INDEX);
		s.addQuery(new TermQuery("id").addTerm("1").addTerm("2").addTerm("3").addTerm("4").exclude());
		s.addQuery(new FilterQuery("country:US"));
		searchResult = zuliaWorkPool.search(s);
		Assertions.assertEquals(28, searchResult.getTotalHits());

		s = new Search(FACET_TEST_INDEX);
		s.addQuery(new TermQuery("id").addTerm("1").addTerm("2").addTerm("3").addTerm("4").exclude());
		s.addQuery(new FilterQuery("country:US").exclude());
		s.addQuery(new MatchAllQuery()); //need to use match all because all other queries are negated
		searchResult = zuliaWorkPool.search(s);
		Assertions.assertEquals(28, searchResult.getTotalHits());
	}

	@Test
	@Order(4)
	public void reindex() throws Exception {
		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("title");
		indexConfig.addFieldConfig(FieldConfigBuilder.create("id", FieldType.STRING).indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.create("title", FieldType.STRING).indexAs(DefaultAnalyzers.STANDARD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.create("issn", FieldType.STRING).indexAs(DefaultAnalyzers.LC_KEYWORD).facet().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.create("eissn", FieldType.STRING).indexAs(DefaultAnalyzers.LC_KEYWORD).facet());
		indexConfig.addFieldConfig(FieldConfigBuilder.create("uid", FieldType.STRING).indexAs(DefaultAnalyzers.LC_KEYWORD));
		indexConfig.addFieldConfig(FieldConfigBuilder.create("an", FieldType.NUMERIC_INT).index().displayName("Accession Number").sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.create("country", FieldType.STRING).indexAs(DefaultAnalyzers.LC_KEYWORD).facet().sort());
		indexConfig.addFieldConfig(
				FieldConfigBuilder.create("date", FieldType.DATE).index().facetAs(DateHandling.DATE_YYYY_MM_DD).description("The very special data").sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.create("testList", FieldType.STRING).index());
		indexConfig.setIndexName(FACET_TEST_INDEX);
		indexConfig.setNumberOfShards(1);

		zuliaWorkPool.createIndex(indexConfig);

		zuliaWorkPool.reindex(new Reindex(FACET_TEST_INDEX));

		Query query = new Query(FACET_TEST_INDEX, null, 0).addCountRequest("eissn");

		QueryResult queryResult = zuliaWorkPool.query(query);

		List<FacetCount> eissnCounts = queryResult.getFacetCounts("eissn");

		Assertions.assertEquals(eissns.length, eissnCounts.size());

		for (FacetCount eissnCount : eissnCounts) {
			Assertions.assertEquals(COUNT_PER_ISSN, eissnCount.getCount());
		}
	}

	@Test
	@Order(5)
	public void restart() throws Exception {
		TestHelper.stopNodes();
		Thread.sleep(2000);
		TestHelper.startNodes();
		Thread.sleep(2000);
	}

	@Test
	@Order(6)
	public void confirm() throws Exception {

		{
			Query q = new Query(FACET_TEST_INDEX, "title:userguide", 0).addCountRequest("issn", 30);
			QueryResult qr = zuliaWorkPool.query(q);

			Assertions.assertEquals(totalRecordsWithTitle, qr.getTotalHits(), "Total record count mismatch");

			Assertions.assertEquals(issns.length - 1, qr.getFacetCounts("issn").size(), "Total facets mismatch");

			for (FacetCount fc : qr.getFacetCounts("issn")) {
				Assertions.assertEquals(COUNT_PER_ISSN, fc.getCount(), "Count for facet <" + fc.getFacet() + "> mismatch");
			}

		}

		{

			Query q = new Query(FACET_TEST_INDEX, "title:userguide", 0).addCountRequest("date", 30);
			QueryResult qr = zuliaWorkPool.query(q);

			Assertions.assertEquals(totalRecordsWithTitle, qr.getTotalHits(), "Total record count mismatch");
			Assertions.assertEquals(3, qr.getFacetCounts("date").size(), "Total facets mismatch");

			for (@SuppressWarnings("unused") FacetCount fc : qr.getFacetCounts("date")) {
				//System.out.println(fc);
			}

		}

		{

			Query q = new Query(FACET_TEST_INDEX, null, 1).addFieldSort("title", ASCENDING, true);
			QueryResult qr = zuliaWorkPool.query(q);

			Assertions.assertEquals(1, qr.getDocuments().size(), "Only one record should be returned");
			Assertions.assertEquals("Facet Userguide", qr.getFirstDocument().get("title"), "First Title should be Facet Userguide");

			q = new Query(FACET_TEST_INDEX, null, 1).addFieldSort("title", ASCENDING, false);
			qr = zuliaWorkPool.query(q);

			Assertions.assertEquals(1, qr.getDocuments().size(), "Only one record should be returned");
			Assertions.assertNull(qr.getFirstDocument().get("title"), "First Title should be null");

			q = new Query(FACET_TEST_INDEX, null, 3).addFieldSort("title", DESCENDING, false);
			qr = zuliaWorkPool.query(q);

			Assertions.assertEquals(3, qr.getDocuments().size(), "Three records should be returned");
			Assertions.assertEquals("Special Userguide", qr.getFirstDocument().get("title"), "First Title should be Special Userguide");

			q = new Query(FACET_TEST_INDEX, null, 3).addFieldSort("title", DESCENDING, true);
			qr = zuliaWorkPool.query(q);

			Assertions.assertEquals(3, qr.getDocuments().size(), "Three records should be returned");
			Assertions.assertNull(qr.getFirstDocument().get("title"), "First Title should be null");

			q = new Query(FACET_TEST_INDEX, null, 1).addFieldSort("an", ASCENDING, true);
			qr = zuliaWorkPool.query(q);

			Assertions.assertEquals(1, qr.getFirstDocument().get("an"), "First AN should be 1");

			q = new Query(FACET_TEST_INDEX, null, 1).addFieldSort("an", DESCENDING, true);
			qr = zuliaWorkPool.query(q);

			Assertions.assertNull(qr.getFirstDocument().get("an"), "First AN should be null");

			q = new Query(FACET_TEST_INDEX, null, 1).addFieldSort("an", ASCENDING, false);
			qr = zuliaWorkPool.query(q);

			Assertions.assertNull(qr.getFirstDocument().get("an"), "First AN should be null");

			q = new Query(FACET_TEST_INDEX, null, 10).addFieldSort("an", DESCENDING, false);
			qr = zuliaWorkPool.query(q);

			Assertions.assertEquals(59, qr.getDocuments().get(0).get("an"), "First AN should be 59");
			Assertions.assertEquals(50, qr.getDocuments().get(9).get("an"), "Tenth AN should be 50");

		}

		{
			Query q = new Query(FACET_TEST_INDEX, "title:userguide", 10);
			q.addDrillDown("issn", "1234-1234").addDrillDown("country", "France");
			q.addCountRequest("issn");

			QueryResult qr = zuliaWorkPool.query(q);

			Assertions.assertEquals(COUNT_PER_ISSN / 2, qr.getTotalHits(), "Total record count after drill down mismatch");
			Assertions.assertEquals(1, qr.getFacetCounts("issn").size(), "Number of issn facets  mismatch");

		}

		{
			Query q = new Query(FACET_TEST_INDEX, "title:userguide", 10).addDrillDown("date", "2014-10-04");

			QueryResult qr = zuliaWorkPool.query(q);

			Assertions.assertEquals(totalRecordsWithTitle / 10, qr.getTotalHits(), "Total record count after drill down mismatch");

		}

		{
			Query q = new Query(FACET_TEST_INDEX, "title:userguide", 10).addDrillDown("date", "2013-09-04");

			QueryResult qr = zuliaWorkPool.query(q);

			Assertions.assertEquals((totalRecordsWithTitle * 2) / 5, qr.getTotalHits(), "Total record count after drill down mismatch");

		}

		{
			Query q = new Query(FACET_TEST_INDEX, "title:userguide", 10).addDrillDown("date", "2012-08-04");

			QueryResult qr = zuliaWorkPool.query(q);

			Assertions.assertEquals(totalRecordsWithTitle / 2, qr.getTotalHits(), "Total record count after drill down mismatch");

		}

		{
			Query q = new Query(FACET_TEST_INDEX, "title:userguide", 10).addDrillDown("issn", "1234-1234");

			QueryResult qr = zuliaWorkPool.query(q);

			Assertions.assertEquals(COUNT_PER_ISSN, qr.getTotalHits(), "Total record count after drill down mismatch");

		}

		{
			Query q = new Query(FACET_TEST_INDEX, "title:userguide", 10).addDrillDown("issn", "1234-1234").addDrillDown("issn", "3333-1234");

			QueryResult qr = zuliaWorkPool.query(q);

			Assertions.assertEquals(COUNT_PER_ISSN * 2, qr.getTotalHits(), "Total record count after drill down mismatch");

		}
		{
			Query q = new Query(FACET_TEST_INDEX, "title:userguide", 10).addDrillDown("issn", "1234-1234").addDrillDown("country", "France");

			QueryResult qr = zuliaWorkPool.query(q);

			Assertions.assertEquals(COUNT_PER_ISSN / 2, qr.getTotalHits(), "Total record count after drill down mismatch");

		}

		{
			Query q = new Query(FACET_TEST_INDEX, "country:US", 10).setResultFetchType(ZuliaQuery.FetchType.META);

			QueryResult qr = zuliaWorkPool.query(q);

			for (ZuliaQuery.ScoredResult result : qr.getResults()) {
				Document metadata = ZuliaUtil.byteStringToMongoDocument(result.getResultDocument().getMetadata());
				Assertions.assertEquals("someValue", metadata.getString("test"));
			}

			Assertions.assertEquals(totalRecords / 2, qr.getTotalHits(), "Total record count filtered on half mismatch");

		}

		{
			Query q = new Query(FACET_TEST_INDEX, "country:US", 10).setResultFetchType(ZuliaQuery.FetchType.FULL);

			QueryResult qr = zuliaWorkPool.query(q);

			for (ZuliaQuery.ScoredResult result : qr.getResults()) {
				Document metadata = ZuliaUtil.byteStringToMongoDocument(result.getResultDocument().getMetadata());
				Assertions.assertEquals("someValue", metadata.getString("test"));
			}

			Assertions.assertEquals(totalRecords / 2, qr.getTotalHits(), "Total record count filtered on half mismatch");

		}

		{
			Query q = new Query(FACET_TEST_INDEX, null, 1).addFieldSort("date", ASCENDING).addFieldSort("issn", ASCENDING);

			QueryResult qr = zuliaWorkPool.query(q);

			Document firstDateDocument = qr.getFirstDocument();

			q = new Query(FACET_TEST_INDEX, null, 1).addFieldSort("date", DESCENDING).addFieldSort("issn", DESCENDING);

			qr = zuliaWorkPool.query(q);

			Document lastDateDocument = qr.getFirstDocument();

			Date firstDate = firstDateDocument.getDate("date");
			Date lastDate = lastDateDocument.getDate("date");

			Assertions.assertTrue(firstDate.compareTo(lastDate) < 0, "First date: " + firstDate + " lastDate: " + lastDate);
		}

	}

	@Test
	@Order(6)
	public void confirmBuilder() throws Exception {
		{

			Search s = new Search(FACET_TEST_INDEX).addQuery(new FilterQuery("title:userguide")).addCountFacet(new CountFacet("issn").setTopN(30));

			SearchResult sr = zuliaWorkPool.search(s);

			Assertions.assertEquals(totalRecordsWithTitle, sr.getTotalHits(), "Total record count mismatch");

			Assertions.assertEquals(issns.length - 1, sr.getFacetCounts("issn").size(), "Total facets mismatch");

			for (FacetCount fc : sr.getFacetCounts("issn")) {
				Assertions.assertEquals(COUNT_PER_ISSN, fc.getCount(), "Count for facet <" + fc.getFacet() + "> mismatch");
			}

		}

		{

			Search s = new Search(FACET_TEST_INDEX).addQuery(new FilterQuery("title:userguide")).addCountFacet(new CountFacet("date").setTopN(30));
			SearchResult sr = zuliaWorkPool.search(s);

			Assertions.assertEquals(totalRecordsWithTitle, sr.getTotalHits(), "Total record count mismatch");
			Assertions.assertEquals(3, sr.getFacetCounts("date").size(), "Total facets mismatch");

			for (@SuppressWarnings("unused") FacetCount fc : sr.getFacetCounts("date")) {
				//System.out.println(fc);
			}

		}

		{

			Search s = new Search(FACET_TEST_INDEX).setAmount(1).addSort(new Sort("title").ascending().missingLast());

			SearchResult sr = zuliaWorkPool.search(s);

			Assertions.assertEquals(1, sr.getDocuments().size(), "Only one record should be returned");
			Assertions.assertEquals("Facet Userguide", sr.getFirstDocument().get("title"), "First Title should be Facet Userguide");

			s.clearSort().addSort(new Sort("title").ascending().missingFirst());
			sr = zuliaWorkPool.search(s);

			Assertions.assertEquals(1, sr.getDocuments().size(), "Only one record should be returned");
			Assertions.assertNull(sr.getFirstDocument().get("title"), "First Title should be null");

			s.setAmount(3).clearSort().addSort(new Sort("title").descending().missingFirst());
			;
			sr = zuliaWorkPool.search(s);

			Assertions.assertEquals(3, sr.getDocuments().size(), "Three records should be returned");
			Assertions.assertEquals("Special Userguide", sr.getFirstDocument().get("title"), "First Title should be Special Userguide");

			s.clearSort().addSort(new Sort("title").descending().missingLast());
			;
			sr = zuliaWorkPool.search(s);

			Assertions.assertEquals(3, sr.getDocuments().size(), "Three records should be returned");
			Assertions.assertNull(sr.getFirstDocument().get("title"), "First Title should be null");

			s = new Search(FACET_TEST_INDEX).setAmount(1).addSort(new Sort("an").ascending().missingLast());
			sr = zuliaWorkPool.search(s);

			Assertions.assertEquals(1, sr.getFirstDocument().get("an"), "First AN should be 1");

			s = new Search(FACET_TEST_INDEX).setAmount(1).addSort(new Sort("an").descending().missingLast());
			sr = zuliaWorkPool.search(s);

			Assertions.assertNull(sr.getFirstDocument().get("an"), "First AN should be null");

			s = new Search(FACET_TEST_INDEX).setAmount(1).addSort(new Sort("an").ascending().missingFirst());
			sr = zuliaWorkPool.search(s);

			Assertions.assertNull(sr.getFirstDocument().get("an"), "First AN should be null");

			s = new Search(FACET_TEST_INDEX).setAmount(10).addSort(new Sort("an").descending().missingFirst());
			sr = zuliaWorkPool.search(s);

			Assertions.assertEquals(59, sr.getDocuments().get(0).get("an"), "First AN should be 59");
			Assertions.assertEquals(50, sr.getDocuments().get(9).get("an"), "Tenth AN should be 50");

		}

		{
			Search s = new Search(FACET_TEST_INDEX).addQuery(new FilterQuery("title:userguide"));
			s.addFacetDrillDown("issn", "1234-1234").addFacetDrillDown("country", "France");
			s.addCountFacet(new CountFacet("issn"));

			SearchResult sr = zuliaWorkPool.search(s);

			Assertions.assertEquals(COUNT_PER_ISSN / 2, sr.getTotalHits(), "Total record count after drill down mismatch");
			Assertions.assertEquals(1, sr.getFacetCounts("issn").size(), "Number of issn facets  mismatch");

		}

		{
			Search s = new Search(FACET_TEST_INDEX).addQuery(new FilterQuery("title:userguide")).addFacetDrillDown("date", "2014-10-04");

			SearchResult sr = zuliaWorkPool.search(s);

			Assertions.assertEquals(totalRecordsWithTitle / 10, sr.getTotalHits(), "Total record count after drill down mismatch");

		}

		{

			Search s = new Search(FACET_TEST_INDEX).addQuery(new FilterQuery("title:userguide")).addFacetDrillDown("date", "2013-09-04");

			SearchResult sr = zuliaWorkPool.search(s);

			Assertions.assertEquals((totalRecordsWithTitle * 2) / 5, sr.getTotalHits(), "Total record count after drill down mismatch");

		}

		{
			Search s = new Search(FACET_TEST_INDEX).addQuery(new FilterQuery("title:userguide")).addFacetDrillDown("date", "2012-08-04");

			SearchResult sr = zuliaWorkPool.search(s);

			Assertions.assertEquals(totalRecordsWithTitle / 2, sr.getTotalHits(), "Total record count after drill down mismatch");

		}

		{
			Search s = new Search(FACET_TEST_INDEX).addQuery(new FilterQuery("title:userguide")).addFacetDrillDown("issn", "1234-1234").setAmount(10);

			SearchResult sr = zuliaWorkPool.search(s);

			Assertions.assertEquals(COUNT_PER_ISSN, sr.getTotalHits(), "Total record count after drill down mismatch");
			Assertions.assertEquals(10, sr.getDocuments().size(), "Ten document should be returned");

		}

		{

			Search s = new Search(FACET_TEST_INDEX).addQuery(new FilterQuery("title:userguide")).addFacetDrillDown("issn", "1234-1234")
					.addFacetDrillDown("issn", "3333-1234");

			SearchResult sr = zuliaWorkPool.search(s);

			Assertions.assertEquals(COUNT_PER_ISSN * 2, sr.getTotalHits(), "Total record count after drill down mismatch");

		}
		{

			Search s = new Search(FACET_TEST_INDEX).addQuery(new ScoredQuery("title:userguide")).addFacetDrillDown("issn", "1234-1234")
					.addFacetDrillDown("country", "France");

			SearchResult sr = zuliaWorkPool.search(s);

			Assertions.assertEquals(COUNT_PER_ISSN / 2, sr.getTotalHits(), "Total record count after drill down mismatch");

		}

		{
			Search s = new Search(FACET_TEST_INDEX).addQuery(new ScoredQuery("country:US")).setAmount(10).setResultFetchType(ZuliaQuery.FetchType.META);

			SearchResult sr = zuliaWorkPool.search(s);

			for (ZuliaQuery.ScoredResult result : sr.getResults()) {

				Document metadata = ZuliaUtil.byteStringToMongoDocument(result.getResultDocument().getMetadata());
				Assertions.assertEquals("someValue", metadata.getString("test"));
			}

			Assertions.assertEquals(totalRecords / 2, sr.getTotalHits(), "Total record count filtered on half mismatch");

			s = new Search(FACET_TEST_INDEX).addQuery(new ScoredQuery("country:US")).setAmount(10).setResultFetchType(ZuliaQuery.FetchType.FULL);

			sr = zuliaWorkPool.search(s);

			for (ZuliaQuery.ScoredResult result : sr.getResults()) {

				Document metadata = ZuliaUtil.byteStringToMongoDocument(result.getResultDocument().getMetadata());
				Assertions.assertEquals("someValue", metadata.getString("test"));
			}

			Assertions.assertEquals(totalRecords / 2, sr.getTotalHits(), "Total record count filtered on half mismatch");

		}

		{
			Search s = new Search(FACET_TEST_INDEX).addQuery(new ScoredQuery("country:US")).setAmount(10).setResultFetchType(ZuliaQuery.FetchType.META);

			SearchResult sr = zuliaWorkPool.search(s);

			for (Document metadata : sr.getMetaDocuments()) {
				Assertions.assertEquals("someValue", metadata.getString("test"));
			}

			Assertions.assertEquals(totalRecords / 2, sr.getTotalHits(), "Total record count filtered on half mismatch");

		}

		{
			Search s = new Search(FACET_TEST_INDEX).setAmount(1).addSort(new Sort("date").ascending()).addSort(new Sort("issn"));

			SearchResult sr = zuliaWorkPool.search(s);

			Document firstDateDocument = sr.getFirstDocument();

			s = new Search(FACET_TEST_INDEX).setAmount(1).addSort(new Sort("date").descending()).addSort(new Sort("issn").descending());

			sr = zuliaWorkPool.search(s);

			Document lastDateDocument = sr.getFirstDocument();

			Date firstDate = firstDateDocument.getDate("date");
			Date lastDate = lastDateDocument.getDate("date");

			Assertions.assertTrue(firstDate.compareTo(lastDate) < 0, "First date: " + firstDate + " lastDate: " + lastDate);
		}

		{
			Search s = new Search(FACET_TEST_INDEX).addQuery(new FilterQuery("title:junkjunkjunk"));
			s.addCountFacet(new CountFacet("issn"));

			SearchResult sr = zuliaWorkPool.search(s);

			List<FacetCount> facetCounts = sr.getFacetCounts("issn");
			Assertions.assertEquals(0, facetCounts.size());

		}

	}

	@Test
	@Order(7)
	public void shutdown() throws Exception {
		TestHelper.stopNodes();
		zuliaWorkPool.shutdown();
	}
}
