package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.CountFacet;
import io.zulia.client.command.builder.DrillDown;
import io.zulia.client.command.builder.Search;
import io.zulia.client.command.builder.StatFacet;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.SearchResult;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.message.ZuliaQuery.FacetCount;
import io.zulia.message.ZuliaQuery.FacetStats;
import io.zulia.server.test.node.shared.NodeExtension;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.opentest4j.AssertionFailedError;

import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class HierarchicalFacetTest {

	@RegisterExtension
	static final NodeExtension nodeExtension = new NodeExtension(3);

	public static final String FACET_TEST_INDEX = "facetTestIndex";

	private final int COUNT_PER_PATH = 10;

	private final String[] paths = new String[] { "1/2/3", "1/3/4", "3/20/13", "a/b/c", "one/two/three", "1", "2/3/blah", "4/5/1000", "a/bee/sea" };

	@Test
	@Order(1)
	public void createIndex() throws Exception {

		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("title");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("title").indexAs(DefaultAnalyzers.STANDARD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("path").indexAs(DefaultAnalyzers.LC_KEYWORD).facetHierarchical().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createDate("date").index().facetHierarchical().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("normalFacet").indexAs(DefaultAnalyzers.LC_KEYWORD).facet().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("normalFacetList").indexAs(DefaultAnalyzers.LC_KEYWORD).facet().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createDouble("rating").index().sort());
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
			for (String path : paths) {
				for (int i = 0; i < COUNT_PER_PATH; i++) {
					indexRecord(id, path, i);
					id++;
				}

				//overwrite a few records with the same values
				for (int i = 1; i <= 2; i++) {
					indexRecord(id - i, path, i);
				}

			}
		}

	}

	private void indexRecord(int id, String path, int i) throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		boolean half = (i % 2 == 0);
		boolean tenth = (i % 10 == 0);

		String uniqueId = "mySpecialId-" + id;

		Document mongoDocument = new Document();
		mongoDocument.put("path", path);
		mongoDocument.put("path2", path);
		mongoDocument.put("normalFacet", path);

		mongoDocument.put("id", id);
		mongoDocument.put("rating", (double) (id % 5) + 1.0);

		if (tenth) { // 1/10 of input

			Date d = Date.from(LocalDate.of(2014, Month.OCTOBER, 4).atStartOfDay(ZoneId.of("UTC")).toInstant());
			mongoDocument.put("date", d);
		}
		else if (half) { // 2/5 of input
			Date d = Date.from(LocalDate.of(2013, Month.SEPTEMBER, 4).atStartOfDay(ZoneId.of("UTC")).toInstant());
			mongoDocument.put("date", d);
			mongoDocument.put("normalFacetList", Arrays.asList("value1", "something"));
		}
		else { // 1/2 of input
			Date d = Date.from(LocalDate.of(2012, 8, 4).atStartOfDay(ZoneId.of("UTC")).toInstant());
			mongoDocument.put("date", d);
			mongoDocument.put("normalFacetList", Arrays.asList("value2", "something2"));
		}

		Store s = new Store(uniqueId, FACET_TEST_INDEX);

		ResultDocBuilder resultDocumentBuilder = ResultDocBuilder.newBuilder().setDocument(mongoDocument);
		s.setResultDocument(resultDocumentBuilder);
		zuliaWorkPool.store(s);

	}

	@Test
	@Order(3)
	public void facetTest() throws Exception {
		facetTestSearch(1);

	}

	private static void facetTestSearch(Integer concurrency) throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		//run the first search with realtime to force seeing latest changes without waiting for a commit
		Search search = new Search(FACET_TEST_INDEX).setRealtime(true);
		if (concurrency != null) {
			search.setConcurrency(concurrency);
		}
		search.addCountFacet(new CountFacet("path"));
		search.addCountFacet(new CountFacet("date"));

		SearchResult queryResult = zuliaWorkPool.search(search);

		List<FacetCount> paths = queryResult.getFacetCounts("path");

		for (FacetCount path : paths) {
			if (path.getFacet().equals("1")) {
				Assertions.assertEquals(30, path.getCount());
			}
			else if (path.getFacet().equals("a")) {
				Assertions.assertEquals(20, path.getCount());
			}
			else if (path.getFacet().equals("2")) {
				Assertions.assertEquals(10, path.getCount());
			}
			else if (path.getFacet().equals("3")) {
				Assertions.assertEquals(10, path.getCount());
			}
			else if (path.getFacet().equals("4")) {
				Assertions.assertEquals(10, path.getCount());
			}
			else if (path.getFacet().equals("one")) {
				Assertions.assertEquals(10, path.getCount());
			}
			else {
				throw new AssertionFailedError("Unexpect path <" + path.getFacet() + ">");
			}
		}

		paths = queryResult.getFacetCounts("date");

		for (FacetCount path : paths) {
			if (path.getFacet().equals("2012")) {
				Assertions.assertEquals(45, path.getCount());
			}
			else if (path.getFacet().equals("2013")) {
				Assertions.assertEquals(36, path.getCount());
			}
			else if (path.getFacet().equals("2014")) {
				Assertions.assertEquals(9, path.getCount());
			}
			else {
				throw new AssertionFailedError("Unexpect path <" + path.getFacet() + ">");
			}
		}

		search = new Search(FACET_TEST_INDEX);
		if (concurrency != null) {
			search.setConcurrency(concurrency);
		}
		search.addCountFacet(new CountFacet("path", "1"));
		search.addCountFacet(new CountFacet("path", "2"));

		queryResult = zuliaWorkPool.search(search);
		paths = queryResult.getFacetCountsForPath("path", "1");
		for (FacetCount path : paths) {
			if (path.getFacet().equals("2")) {
				Assertions.assertEquals(10, path.getCount());
			}
			else if (path.getFacet().equals("3")) {
				Assertions.assertEquals(10, path.getCount());
			}
			else {
				throw new AssertionFailedError("Unexpect path <" + path.getFacet() + ">");
			}
		}
		paths = queryResult.getFacetCountsForPath("path", "2");
		for (FacetCount path : paths) {
			if (path.getFacet().equals("3")) {
				Assertions.assertEquals(10, path.getCount());
			}
			else {
				throw new AssertionFailedError("Unexpect path <" + path.getFacet() + ">");
			}
		}

		search = new Search(FACET_TEST_INDEX);
		if (concurrency != null) {
			search.setConcurrency(concurrency);
		}
		search.addCountFacet(new CountFacet("path", "one", "two"));
		queryResult = zuliaWorkPool.search(search);
		paths = queryResult.getFacetCountsForPath("path", "one", "two");
		for (FacetCount path : paths) {
			if (path.getFacet().equals("three")) {
				Assertions.assertEquals(10, path.getCount());
			}
			else {
				throw new AssertionFailedError("Unexpect path <" + path.getFacet() + ">");
			}
		}

		search = new Search(FACET_TEST_INDEX);
		if (concurrency != null) {
			search.setConcurrency(concurrency);
		}
		search.addCountFacet(new CountFacet("path2"));
		Search finalSearch = search;
		Assertions.assertThrows(Exception.class, () -> zuliaWorkPool.search(finalSearch), "path2 is not defined as a facetable field");
	}

	@Test
	@Order(4)
	public void dateHierarchicalFacetTest() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Test date hierarchy at month level
		{
			Search search = new Search(FACET_TEST_INDEX);
			// Query months within year 2012
			search.addCountFacet(new CountFacet("date", "2012"));
			// Query months within year 2013
			search.addCountFacet(new CountFacet("date", "2013"));
			// Query months within year 2014
			search.addCountFacet(new CountFacet("date", "2014"));

			SearchResult queryResult = zuliaWorkPool.search(search);

			// 2012 has month 8 (August)
			List<FacetCount> months2012 = queryResult.getFacetCountsForPath("date", "2012");
			Assertions.assertEquals(1, months2012.size(), "Expected one month for 2012");
			Assertions.assertEquals("8", months2012.getFirst().getFacet());
			Assertions.assertEquals(45, months2012.getFirst().getCount());

			// 2013 has month 9 (September)
			List<FacetCount> months2013 = queryResult.getFacetCountsForPath("date", "2013");
			Assertions.assertEquals(1, months2013.size(), "Expected one month for 2013");
			Assertions.assertEquals("9", months2013.getFirst().getFacet());
			Assertions.assertEquals(36, months2013.getFirst().getCount());

			// 2014 has month 10 (October)
			List<FacetCount> months2014 = queryResult.getFacetCountsForPath("date", "2014");
			Assertions.assertEquals(1, months2014.size(), "Expected one month for 2014");
			Assertions.assertEquals("10", months2014.getFirst().getFacet());
			Assertions.assertEquals(9, months2014.getFirst().getCount());
		}

		// Test date hierarchy at day level
		{
			Search search = new Search(FACET_TEST_INDEX);
			search.addCountFacet(new CountFacet("date", "2012", "8"));
			search.addCountFacet(new CountFacet("date", "2013", "9"));

			SearchResult queryResult = zuliaWorkPool.search(search);

			// 2012/8 has day 4
			List<FacetCount> days2012Aug = queryResult.getFacetCountsForPath("date", "2012", "8");
			Assertions.assertEquals(1, days2012Aug.size(), "Expected one day for 2012/8");
			Assertions.assertEquals("4", days2012Aug.getFirst().getFacet());
			Assertions.assertEquals(45, days2012Aug.getFirst().getCount());

			// 2013/9 has day 4
			List<FacetCount> days2013Sep = queryResult.getFacetCountsForPath("date", "2013", "9");
			Assertions.assertEquals(1, days2013Sep.size(), "Expected one day for 2013/9");
			Assertions.assertEquals("4", days2013Sep.getFirst().getFacet());
			Assertions.assertEquals(36, days2013Sep.getFirst().getCount());
		}
	}

	@Test
	@Order(5)
	public void hierarchicalDrillDownTest() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Drill down to top-level path "1" — should match paths: 1/2/3, 1/3/4, and 1 (bare)
		{
			Search s = new Search(FACET_TEST_INDEX);
			s.addFacetDrillDown(new DrillDown("path").addValue("1"));
			SearchResult sr = zuliaWorkPool.search(s);
			Assertions.assertEquals(30, sr.getTotalHits(), "Drill down to path '1' should match 30 docs");
		}

		// Drill down to "1/2" — should match paths: 1/2/3 only
		{
			Search s = new Search(FACET_TEST_INDEX);
			s.addFacetDrillDown(new DrillDown("path").addValue("1", "2"));
			SearchResult sr = zuliaWorkPool.search(s);
			Assertions.assertEquals(10, sr.getTotalHits(), "Drill down to path '1/2' should match 10 docs");
		}

		// Drill down to "1/2/3" — should match exact path 1/2/3
		{
			Search s = new Search(FACET_TEST_INDEX);
			s.addFacetDrillDown(new DrillDown("path").addValue("1", "2", "3"));
			SearchResult sr = zuliaWorkPool.search(s);
			Assertions.assertEquals(10, sr.getTotalHits(), "Drill down to path '1/2/3' should match 10 docs");
		}

		// Drill down to "a" — should match paths: a/b/c, a/bee/sea
		{
			Search s = new Search(FACET_TEST_INDEX);
			s.addFacetDrillDown(new DrillDown("path").addValue("a"));
			SearchResult sr = zuliaWorkPool.search(s);
			Assertions.assertEquals(20, sr.getTotalHits(), "Drill down to path 'a' should match 20 docs");
		}

		// OR drill down: "1/2" OR "a/b" — 10 + 10 = 20
		{
			Search s = new Search(FACET_TEST_INDEX);
			s.addFacetDrillDown(new DrillDown("path").any().addValue("1", "2").addValue("a", "b"));
			SearchResult sr = zuliaWorkPool.search(s);
			Assertions.assertEquals(20, sr.getTotalHits(), "Drill down to '1/2' OR 'a/b' should match 20 docs");
		}

		// Exclude drill down: NOT path "1" — total is 90, under "1" is 30, so 60
		{
			Search s = new Search(FACET_TEST_INDEX);
			s.addFacetDrillDown(new DrillDown("path").addValue("1").exclude());
			SearchResult sr = zuliaWorkPool.search(s);
			Assertions.assertEquals(60, sr.getTotalHits(), "Exclude drill down on path '1' should match 60 docs");
		}

		// Drill down on date hierarchy: year 2012
		{
			Search s = new Search(FACET_TEST_INDEX);
			s.addFacetDrillDown(new DrillDown("date").addValue("2012"));
			SearchResult sr = zuliaWorkPool.search(s);
			Assertions.assertEquals(45, sr.getTotalHits(), "Drill down to date year '2012' should match 45 docs");
		}

		// Drill down on date hierarchy: year 2012, month 8
		{
			Search s = new Search(FACET_TEST_INDEX);
			s.addFacetDrillDown(new DrillDown("date").addValue("2012", "8"));
			SearchResult sr = zuliaWorkPool.search(s);
			Assertions.assertEquals(45, sr.getTotalHits(), "Drill down to date '2012/8' should match 45 docs");
		}

		// Drill down on date hierarchy: year 2012, month 8, day 4
		{
			Search s = new Search(FACET_TEST_INDEX);
			s.addFacetDrillDown(new DrillDown("date").addValue("2012", "8", "4"));
			SearchResult sr = zuliaWorkPool.search(s);
			Assertions.assertEquals(45, sr.getTotalHits(), "Drill down to date '2012/8/4' should match 45 docs");
		}

		// Combined: drill down path AND date
		{
			Search s = new Search(FACET_TEST_INDEX);
			s.addFacetDrillDown(new DrillDown("path").addValue("1"));
			s.addFacetDrillDown(new DrillDown("date").addValue("2012"));
			SearchResult sr = zuliaWorkPool.search(s);
			// path "1" has 30 docs, half of all docs have date 2012 — 30 * 5/10 = 15
			Assertions.assertEquals(15, sr.getTotalHits(), "Combined drill down on path '1' AND date '2012'");
		}

		// Drill down with count facet: drill on path "1", count children of "1"
		{
			Search s = new Search(FACET_TEST_INDEX);
			s.addFacetDrillDown(new DrillDown("path").addValue("1"));
			s.addCountFacet(new CountFacet("path", "1"));
			SearchResult sr = zuliaWorkPool.search(s);
			Assertions.assertEquals(30, sr.getTotalHits());
			List<FacetCount> children = sr.getFacetCountsForPath("path", "1");
			// Under "1": "2" (from 1/2/3) and "3" (from 1/3/4)
			Assertions.assertEquals(2, children.size());
			for (FacetCount fc : children) {
				if ("2".equals(fc.getFacet()) || "3".equals(fc.getFacet())) {
					Assertions.assertEquals(10, fc.getCount());
				}
				else {
					throw new AssertionFailedError("Unexpected child path <" + fc.getFacet() + ">");
				}
			}
		}
	}

	@Test
	@Order(6)
	public void hierarchicalStatFacetTest() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Stat facet at top level of path hierarchy
		{
			Search search = new Search(FACET_TEST_INDEX);
			search.addStat(new StatFacet("rating", "path"));
			SearchResult sr = zuliaWorkPool.search(search);

			List<FacetStats> statsByPath = sr.getFacetFieldStat("rating", "path");
			Assertions.assertNotNull(statsByPath, "Stat facet results should not be null");
			Assertions.assertFalse(statsByPath.isEmpty(), "Stat facet results should not be empty");

			// Verify that top-level path segments are returned with stats
			boolean found1 = false;
			boolean foundA = false;
			for (FacetStats fs : statsByPath) {
				if ("1".equals(fs.getFacet())) {
					found1 = true;
					Assertions.assertEquals(30, fs.getAllDocCount(), "Path '1' should have 30 docs");
				}
				else if ("a".equals(fs.getFacet())) {
					foundA = true;
					Assertions.assertEquals(20, fs.getAllDocCount(), "Path 'a' should have 20 docs");
				}
			}
			Assertions.assertTrue(found1, "Should have stats for path '1'");
			Assertions.assertTrue(foundA, "Should have stats for path 'a'");
		}

		// Stat facet at second level of path hierarchy
		{
			Search search = new Search(FACET_TEST_INDEX);
			search.addStat(new StatFacet("rating", "path", "1"));
			SearchResult sr = zuliaWorkPool.search(search);

			List<FacetStats> statsByPath = sr.getFacetFieldStat("rating", "path", List.of("1"));
			Assertions.assertNotNull(statsByPath, "Stat facet results for path '1' should not be null");
			Assertions.assertFalse(statsByPath.isEmpty(), "Stat facet results for path '1' should not be empty");

			for (FacetStats fs : statsByPath) {
				if ("2".equals(fs.getFacet())) {
					Assertions.assertEquals(10, fs.getAllDocCount(), "Path '1/2' should have 10 docs");
				}
				else if ("3".equals(fs.getFacet())) {
					Assertions.assertEquals(10, fs.getAllDocCount(), "Path '1/3' should have 10 docs");
				}
				else {
					throw new AssertionFailedError("Unexpected child path <" + fs.getFacet() + ">");
				}
			}
		}

		// Stat facet on date hierarchy (top level — years)
		{
			Search search = new Search(FACET_TEST_INDEX);
			search.addStat(new StatFacet("rating", "date"));
			SearchResult sr = zuliaWorkPool.search(search);

			List<FacetStats> statsByDate = sr.getFacetFieldStat("rating", "date");
			Assertions.assertNotNull(statsByDate, "Stat facet results for date should not be null");
			Assertions.assertFalse(statsByDate.isEmpty(), "Stat facet results for date should not be empty");

			boolean found2012 = false;
			for (FacetStats fs : statsByDate) {
				if ("2012".equals(fs.getFacet())) {
					found2012 = true;
					Assertions.assertEquals(45, fs.getAllDocCount(), "Year 2012 should have 45 docs");
				}
			}
			Assertions.assertTrue(found2012, "Should have stats for year 2012");
		}
	}

	@Test
	@Order(7)
	public void reindex() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("title");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("title").indexAs(DefaultAnalyzers.STANDARD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("path").indexAs(DefaultAnalyzers.LC_KEYWORD).facetHierarchical().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("path2").indexAs(DefaultAnalyzers.LC_KEYWORD).facetHierarchical().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createDate("date").index().facetHierarchical().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createDouble("rating").index().sort());
		indexConfig.setIndexName(FACET_TEST_INDEX);
		indexConfig.setNumberOfShards(1);
		indexConfig.setShardCommitInterval(20); //force some commits

		zuliaWorkPool.createIndex(indexConfig);

		//trigger indexing again with path2 added in the index config
		index();

	}

	@Test
	@Order(8)
	public void restart() throws Exception {
		nodeExtension.restartNodes();
	}

	@Test
	@Order(9)
	public void confirm() throws Exception {
		confirmSearch(1);
		confirmSearch(5);
	}

	private static void confirmSearch(Integer concurrency) throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		Search search = new Search(FACET_TEST_INDEX);
		if (concurrency != null) {
			search.setConcurrency(concurrency);
		}
		String pathField = "path2";
		search.addCountFacet(new CountFacet(pathField));
		search.addCountFacet(new CountFacet("date"));

		SearchResult queryResult = zuliaWorkPool.search(search);

		List<FacetCount> paths = queryResult.getFacetCounts(pathField);

		for (FacetCount path : paths) {
			if (path.getFacet().equals("1")) {
				Assertions.assertEquals(30, path.getCount());
			}
			else if (path.getFacet().equals("a")) {
				Assertions.assertEquals(20, path.getCount());
			}
			else if (path.getFacet().equals("2")) {
				Assertions.assertEquals(10, path.getCount());
			}
			else if (path.getFacet().equals("3")) {
				Assertions.assertEquals(10, path.getCount());
			}
			else if (path.getFacet().equals("4")) {
				Assertions.assertEquals(10, path.getCount());
			}
			else if (path.getFacet().equals("one")) {
				Assertions.assertEquals(10, path.getCount());
			}
			else {
				throw new AssertionFailedError("Unexpect path <" + path.getFacet() + ">");
			}
		}

		paths = queryResult.getFacetCounts("date");

		for (FacetCount path : paths) {
			if (path.getFacet().equals("2012")) {
				Assertions.assertEquals(45, path.getCount());
			}
			else if (path.getFacet().equals("2013")) {
				Assertions.assertEquals(36, path.getCount());
			}
			else if (path.getFacet().equals("2014")) {
				Assertions.assertEquals(9, path.getCount());
			}
			else {
				throw new AssertionFailedError("Unexpect path <" + path.getFacet() + ">");
			}
		}

		search = new Search(FACET_TEST_INDEX);
		if (concurrency != null) {
			search.setConcurrency(concurrency);
		}
		search.addCountFacet(new CountFacet(pathField, "1"));

		queryResult = zuliaWorkPool.search(search);
		paths = queryResult.getFacetCountsForPath(pathField, "1");
		for (FacetCount path : paths) {
			if (path.getFacet().equals("2")) {
				Assertions.assertEquals(10, path.getCount());
			}
			else if (path.getFacet().equals("3")) {
				Assertions.assertEquals(10, path.getCount());
			}
			else {
				throw new AssertionFailedError("Unexpect path <" + path.getFacet() + ">");
			}
		}

		search = new Search(FACET_TEST_INDEX);
		if (concurrency != null) {
			search.setConcurrency(concurrency);
		}
		search.addCountFacet(new CountFacet(pathField, "one", "two"));
		queryResult = zuliaWorkPool.search(search);
		paths = queryResult.getFacetCountsForPath(pathField, "one", "two");
		for (FacetCount path : paths) {
			if (path.getFacet().equals("three")) {
				Assertions.assertEquals(10, path.getCount());
			}
			else {
				throw new AssertionFailedError("Unexpect path <" + path.getFacet() + ">");
			}
		}
	}

}
