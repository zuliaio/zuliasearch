package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.ZuliaFieldConstants;
import io.zulia.client.command.Reindex;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.CountFacet;
import io.zulia.client.command.builder.DrillDown;
import io.zulia.client.command.builder.FilterQuery;
import io.zulia.client.command.builder.MatchAllQuery;
import io.zulia.client.command.builder.ScoredQuery;
import io.zulia.client.command.builder.Search;
import io.zulia.client.command.builder.Sort;
import io.zulia.client.command.builder.TermQuery;
import io.zulia.client.command.factory.FilterFactory;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.CompleteResult;
import io.zulia.client.result.GetIndexConfigResult;
import io.zulia.client.result.SearchResult;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.message.ZuliaIndex.FacetAs.DateHandling;
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

import java.time.LocalDate;
import java.time.Month;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class StartStopTest {

	@RegisterExtension
	static final NodeExtension nodeExtension = new NodeExtension(3);

	public static final String FACET_TEST_INDEX = "ssTest";

	private final int COUNT_PER_ISSN = 10;
	private final String uniqueIdPrefix = "myId-";

	private final String[] issns = new String[] { "1234-1234", "3333-1234", "1234-5555", "1234-4444", "2222-2222", "3331-3333" };
	private final String[] eissns = new String[] { "3234-1234", "4333-1234", "5234-5555", "6234-4444", "9222-2222", "3431-3638" };

	private final int totalRecords = COUNT_PER_ISSN * issns.length;
	private final int totalRecordsWithTitle = COUNT_PER_ISSN * (issns.length - 1);

	@Test
	@Order(1)
	public void createIndex() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("title");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD));
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("title").indexAs(DefaultAnalyzers.STANDARD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("issn").indexAs(DefaultAnalyzers.LC_KEYWORD).facet().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("eissn").indexAs(DefaultAnalyzers.LC_KEYWORD));
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("uid").indexAs(DefaultAnalyzers.LC_KEYWORD));
		indexConfig.addFieldConfig(FieldConfigBuilder.createInt("an").index().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("country").indexAs(DefaultAnalyzers.LC_KEYWORD).facet());
		indexConfig.addFieldConfig(FieldConfigBuilder.createDate("date").index().facetAs(DateHandling.DATE_YYYY_MM_DD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("testList").indexAs(DefaultAnalyzers.STANDARD));
		indexConfig.addFieldConfig(FieldConfigBuilder.createBool("testBool").index().facet().sort());
		indexConfig.setIndexName(FACET_TEST_INDEX);
		indexConfig.setNumberOfShards(1);
		indexConfig.setShardCommitInterval(20); //force some commits

		//optional meta
		indexConfig.setMeta(new Document().append("createTime", new Date()).append("myLabel", "greatLabel"));
		zuliaWorkPool.createIndex(indexConfig);

		GetIndexConfigResult indexConfigResult = zuliaWorkPool.getIndexConfig(FACET_TEST_INDEX);
		ClientIndexConfig storedClientIndexConfig = indexConfigResult.getIndexConfig();
		Assertions.assertEquals(indexConfig.getIndexName(), storedClientIndexConfig.getIndexName());
		Assertions.assertEquals(indexConfig.getDefaultSearchFields(), storedClientIndexConfig.getDefaultSearchFields());
		Assertions.assertEquals(indexConfig.getNumberOfShards(), storedClientIndexConfig.getNumberOfShards());
		Assertions.assertEquals(indexConfig.getFieldConfigMap(), storedClientIndexConfig.getFieldConfigMap());
		Assertions.assertEquals(indexConfig.getMeta(), storedClientIndexConfig.getMeta());
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

		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
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
			mongoDocument.put("date", LocalDate.of(2014, Month.OCTOBER, 4));
		}
		else if (half) { // 2/5 of input
			mongoDocument.put("date", LocalDate.of(2013, Month.SEPTEMBER, 4));
		}
		else { // 1/2 of input
			mongoDocument.put("date", LocalDate.of(2012, 8, 4));
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
	public void sortScoreBuilder() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		Search search = new Search(FACET_TEST_INDEX).setAmount(10);
		search.addQuery(new ScoredQuery("issn:\"1234-1234\" OR country:US"));
		search.addSort(new Sort(ZuliaFieldConstants.SCORE_FIELD).ascending());

		SearchResult searchResult = zuliaWorkPool.search(search);

		double lowScore = -1;
		double highScore = -1;
		for (CompleteResult result : searchResult.getCompleteResults()) {
			Assertions.assertTrue(result.getScore() > 0);
			if (lowScore < 0 || result.getScore() < lowScore) {
				lowScore = result.getScore();
			}
		}

		search.clearSort();
		search.addSort(new Sort(ZuliaFieldConstants.SCORE_FIELD).descending());
		searchResult = zuliaWorkPool.search(search);

		for (CompleteResult result : searchResult.getCompleteResults()) {
			Assertions.assertTrue(result.getScore() > 0);
			if (highScore < 0 || result.getScore() > highScore) {
				highScore = result.getScore();
			}
		}

		Assertions.assertTrue(highScore > lowScore);

	}

	@Test
	@Order(3)
	public void lengthTestBuilder() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
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
	public void boolTestBuilder() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
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

		s = new Search(FACET_TEST_INDEX).addCountFacet(new CountFacet("testBool"));
		searchResult = zuliaWorkPool.search(s);
		List<FacetCount> facetCounts = searchResult.getFacetCounts("testBool");
		Assertions.assertEquals(2, facetCounts.size());
		Assertions.assertEquals(6, facetCounts.get(0).getCount());
		Assertions.assertEquals("True", facetCounts.get(0).getFacet());
		Assertions.assertEquals(4, facetCounts.get(1).getCount());
		Assertions.assertEquals("False", facetCounts.get(1).getFacet());
	}

	@Test
	@Order(3)
	public void termTestBuilder() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
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
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("title");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort()); // make it sortable this time
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("title").indexAs(DefaultAnalyzers.STANDARD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("issn").indexAs(DefaultAnalyzers.LC_KEYWORD).facet().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("eissn").indexAs(DefaultAnalyzers.LC_KEYWORD).facet());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("uid").indexAs(DefaultAnalyzers.LC_KEYWORD));
		indexConfig.addFieldConfig(FieldConfigBuilder.createInt("an").index().displayName("Accession Number").sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("country").indexAs(DefaultAnalyzers.LC_KEYWORD).facet().sort());
		indexConfig.addFieldConfig(
				FieldConfigBuilder.createDate("date").index().facetAs(DateHandling.DATE_YYYY_MM_DD).description("The very special data").sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("testList").indexAs(DefaultAnalyzers.STANDARD).facet());
		indexConfig.addFieldConfig(FieldConfigBuilder.createBool("testBool").index().facet().sort());
		indexConfig.setIndexName(FACET_TEST_INDEX);
		indexConfig.setNumberOfShards(1);

		zuliaWorkPool.createIndex(indexConfig);

		zuliaWorkPool.reindex(new Reindex(FACET_TEST_INDEX));

		Search search = new Search(FACET_TEST_INDEX).addCountFacet(new CountFacet("eissn"));

		SearchResult searchResult = zuliaWorkPool.search(search);

		List<FacetCount> eissnCounts = searchResult.getFacetCounts("eissn");

		Assertions.assertEquals(eissns.length, eissnCounts.size());

		for (FacetCount eissnCount : eissnCounts) {
			Assertions.assertEquals(COUNT_PER_ISSN, eissnCount.getCount());
		}
	}

	@Test
	@Order(5)
	public void restart() throws Exception {
		nodeExtension.restartNodes();
	}

	@Test
	@Order(6)
	public void confirmBuilder() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
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
			s.addFacetDrillDown(new DrillDown("issn").addValue("1234-1234")).addFacetDrillDown(new DrillDown("country").addValue("France"));
			s.addCountFacet(new CountFacet("issn"));

			SearchResult sr = zuliaWorkPool.search(s);

			Assertions.assertEquals(COUNT_PER_ISSN / 2, sr.getTotalHits(), "Total record count after drill down mismatch");
			Assertions.assertEquals(1, sr.getFacetCounts("issn").size(), "Number of issn facets mismatch");

		}

		{
			Search s = new Search(FACET_TEST_INDEX).addQuery(new FilterQuery("title:userguide"))
					.addFacetDrillDown(new DrillDown("date").addValue("2014-10-04"));

			SearchResult sr = zuliaWorkPool.search(s);

			Assertions.assertEquals(totalRecordsWithTitle / 10, sr.getTotalHits(), "Total record count after drill down mismatch");

		}

		{

			Search s = new Search(FACET_TEST_INDEX).addQuery(new FilterQuery("title:userguide"))
					.addFacetDrillDown(new DrillDown("date").addValue("2013-09-04"));

			SearchResult sr = zuliaWorkPool.search(s);

			Assertions.assertEquals((totalRecordsWithTitle * 2L) / 5, sr.getTotalHits(), "Total record count after drill down mismatch");

		}

		{

			Search s = new Search(FACET_TEST_INDEX).addFacetDrillDown(new DrillDown("testList").addValues("one", "two").all()).setAmount(1);

			SearchResult sr = zuliaWorkPool.search(s);

			Assertions.assertEquals((totalRecords / 2), sr.getTotalHits(), "Total record count after drill down mismatch");

			List<String> testList = sr.getFirstDocument().getList("testList", String.class);
			Assertions.assertTrue(testList.contains("one"));
			Assertions.assertTrue(testList.contains("two"));
		}

		{

			Search s = new Search(FACET_TEST_INDEX).addFacetDrillDown(new DrillDown("testList").addValues("one", "two").all().exclude()).setAmount(1);
			s.setDebug(true);
			SearchResult sr = zuliaWorkPool.search(s);

			Assertions.assertEquals((totalRecords / 2), sr.getTotalHits(), "Total record count after drill down mismatch");

			List<String> testList = sr.getFirstDocument().getList("testList", String.class);
			Assertions.assertTrue(testList.contains("a"));
			Assertions.assertTrue(testList.contains("b"));
			Assertions.assertTrue(testList.contains("c"));
		}

		{

			Search s = new Search(FACET_TEST_INDEX).addFacetDrillDown(new DrillDown("testList").addValues("one", "a").all());

			SearchResult sr = zuliaWorkPool.search(s);

			Assertions.assertEquals(0, sr.getTotalHits(), "Total record count after drill down mismatch");

		}

		{

			Search s = new Search(FACET_TEST_INDEX).addFacetDrillDown(new DrillDown("testList").addValues("one", "a").exclude().all());

			SearchResult sr = zuliaWorkPool.search(s);

			Assertions.assertEquals(totalRecords, sr.getTotalHits(), "Total record count after drill down mismatch");

		}

		{
			Search s = new Search(FACET_TEST_INDEX).addQuery(new FilterQuery("title:userguide"))
					.addFacetDrillDown(new DrillDown("date").addValue("2012-08-04"));

			SearchResult sr = zuliaWorkPool.search(s);

			Assertions.assertEquals(totalRecordsWithTitle / 2, sr.getTotalHits(), "Total record count after drill down mismatch");

		}

		{
			Search s = new Search(FACET_TEST_INDEX).addQuery(new FilterQuery("title:userguide")).addFacetDrillDown(new DrillDown("issn").addValue("1234-1234"))
					.setAmount(10);

			SearchResult sr = zuliaWorkPool.search(s);

			Assertions.assertEquals(COUNT_PER_ISSN, sr.getTotalHits(), "Total record count after drill down mismatch");
			Assertions.assertEquals(10, sr.getDocuments().size(), "Ten document should be returned");

		}

		{

			Search s = new Search(FACET_TEST_INDEX).addQuery(new FilterQuery("title:userguide"))
					.addFacetDrillDown(new DrillDown("issn").addValues("1234-1234", "3333-1234"));

			SearchResult sr = zuliaWorkPool.search(s);

			Assertions.assertEquals(COUNT_PER_ISSN * 2, sr.getTotalHits(), "Total record count after drill down mismatch");

		}
		{

			Search s = new Search(FACET_TEST_INDEX).addQuery(new ScoredQuery("title:userguide")).addFacetDrillDown(new DrillDown("issn").addValue("1234-1234"))
					.addFacetDrillDown(new DrillDown("country").addValue("France"));

			SearchResult sr = zuliaWorkPool.search(s);

			Assertions.assertEquals(COUNT_PER_ISSN / 2, sr.getTotalHits(), "Total record count after drill down mismatch");

		}

		{
			Search s = new Search(FACET_TEST_INDEX).addQuery(new ScoredQuery("country:US")).setAmount(10).setResultFetchType(ZuliaQuery.FetchType.META);

			SearchResult sr = zuliaWorkPool.search(s);

			for (CompleteResult result : sr.getCompleteResults()) {

				Document metadata = result.getMetadata();
				Assertions.assertEquals("someValue", metadata.getString("test"));
			}

			Assertions.assertEquals(totalRecords / 2, sr.getTotalHits(), "Total record count filtered on half mismatch");

			s = new Search(FACET_TEST_INDEX).addQuery(new ScoredQuery("country:US")).setAmount(10).setResultFetchType(ZuliaQuery.FetchType.ALL);

			sr = zuliaWorkPool.search(s);

			for (CompleteResult result : sr.getCompleteResults()) {

				Document metadata = result.getMetadata();
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

		{
			Search s = new Search(FACET_TEST_INDEX).addQuery(
					FilterFactory.rangeLocalDate("date").setRange(LocalDate.of(2014, 1, 1), LocalDate.of(2014, 12, 31)).toQuery());

			SearchResult sr = zuliaWorkPool.search(s);

			Assertions.assertEquals(totalRecords / 10, sr.getTotalHits());

		}

		{
			Search s = new Search(FACET_TEST_INDEX).addQuery(
					FilterFactory.rangeLocalDate("date").setRange(LocalDate.of(2014, 10, 5), LocalDate.of(2014, 12, 31)).toQuery());

			SearchResult sr = zuliaWorkPool.search(s);

			Assertions.assertEquals(0, sr.getTotalHits());

		}

		{
			Search s = new Search(FACET_TEST_INDEX).addQuery(FilterFactory.rangeLocalDate("date").setMinValue(LocalDate.of(2014, 10, 5)).toQuery());

			SearchResult sr = zuliaWorkPool.search(s);

			Assertions.assertEquals(0, sr.getTotalHits());

		}

		{
			Search s = new Search(FACET_TEST_INDEX).addQuery(FilterFactory.rangeLocalDate("date").setMaxValue(LocalDate.of(2014, 10, 5)).toQuery());

			SearchResult sr = zuliaWorkPool.search(s);

			Assertions.assertEquals(totalRecords, sr.getTotalHits());

		}

	}

	@Test
	@Order(57)
	public void confirmTermInSet() throws Exception {
		//try it again with id being sortable
		termTestBuilder();
	}

}
