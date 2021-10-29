package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.CountFacet;
import io.zulia.client.command.builder.Search;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.QueryResult;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.message.ZuliaIndex.FieldConfig.FieldType;
import io.zulia.message.ZuliaQuery.FacetCount;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.opentest4j.AssertionFailedError;

import java.time.LocalDate;
import java.time.Month;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class HierarchicalFacetTest {

	public static final String FACET_TEST_INDEX = "facetTestIndex";

	private final int COUNT_PER_PATH = 10;

	private final String[] paths = new String[] { "1/2/3", "1/3/4", "3/20/13", "a/b/c", "one/two/three", "1", "2/3/blah", "4/5/1000", "a/bee/sea" };

	private final int totalRecords = COUNT_PER_PATH * paths.length;

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
		indexConfig.addFieldConfig(FieldConfigBuilder.create("path", FieldType.STRING).indexAs(DefaultAnalyzers.LC_KEYWORD).facetHierarchical().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.create("date", FieldType.DATE).index().facetHierarchical().sort());
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
			for (int j = 0; j < paths.length; j++) {
				String path = paths[j];

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
		boolean half = (i % 2 == 0);
		boolean tenth = (i % 10 == 0);

		String uniqueId = "mySpecialId-" + id;

		Document mongoDocument = new Document();
		mongoDocument.put("path", path);
		mongoDocument.put("path2", path);

		mongoDocument.put("id", id);

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
		s.setResultDocument(resultDocumentBuilder);
		zuliaWorkPool.store(s);

	}

	@Test
	@Order(3)
	public void facetTest() throws Exception {
		Search search = new Search(FACET_TEST_INDEX);
		search.addCountFacet(new CountFacet("path"));
		search.addCountFacet(new CountFacet("date"));

		QueryResult queryResult = zuliaWorkPool.search(search);

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
		search.addCountFacet(new CountFacet("path2"));
		Search finalSearch = search;
		Assertions.assertThrows(Exception.class, () -> zuliaWorkPool.search(finalSearch), "path2 is not defined as a facetable field");

	}

	@Test
	@Order(4)
	public void reindex() throws Exception {
		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("title");
		indexConfig.addFieldConfig(FieldConfigBuilder.create("id", FieldType.STRING).indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.create("title", FieldType.STRING).indexAs(DefaultAnalyzers.STANDARD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.create("path", FieldType.STRING).indexAs(DefaultAnalyzers.LC_KEYWORD).facetHierarchical().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.create("path2", FieldType.STRING).indexAs(DefaultAnalyzers.LC_KEYWORD).facetHierarchical().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.create("date", FieldType.DATE).index().facetHierarchical().sort());
		indexConfig.setIndexName(FACET_TEST_INDEX);
		indexConfig.setNumberOfShards(1);
		indexConfig.setShardCommitInterval(20); //force some commits

		zuliaWorkPool.createIndex(indexConfig);

		//trigger indexing again with path2 added in the index config
		index();

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
		Search search = new Search(FACET_TEST_INDEX);
		String pathField = "path2";
		search.addCountFacet(new CountFacet(pathField));
		search.addCountFacet(new CountFacet("date"));

		QueryResult queryResult = zuliaWorkPool.search(search);

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

	@Test
	@Order(7)
	public void shutdown() throws Exception {
		TestHelper.stopNodes();
		zuliaWorkPool.shutdown();
	}
}
