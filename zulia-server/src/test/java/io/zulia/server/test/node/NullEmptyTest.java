package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.ScoredQuery;
import io.zulia.client.command.builder.Search;
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

import java.util.List;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class NullEmptyTest {

	@RegisterExtension
	static final NodeExtension nodeExtension = new NodeExtension(3);

	public static final String NUlL_TEST_INDEX = "nullTest";

	private static final int repeatCount = 50;
	private static final int uniqueDocs = 7;

	@Test
	@Order(1)
	public void createIndex() throws Exception {

		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("title");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("title").indexAs(DefaultAnalyzers.STANDARD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("comments").indexAs(DefaultAnalyzers.STANDARD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createDouble("rating").index().sort());
		indexConfig.setIndexName(NUlL_TEST_INDEX);
		indexConfig.setNumberOfShards(1);
		indexConfig.setShardCommitInterval(20); //force some commits

		zuliaWorkPool.createIndex(indexConfig);
	}

	@Test
	@Order(2)
	public void index() throws Exception {

		for (int i = 0; i < repeatCount; i++) {

			indexRecord(i * uniqueDocs, "something special but boring", null, 1.0);
			indexRecord(i * uniqueDocs + 1, "something really special", List.of("reddish and blueish", "the best", "so great"), 2.4);
			indexRecord(i * uniqueDocs + 2, "", List.of("pink with big big big stripes", ""), 5.0);
			indexRecord(i * uniqueDocs + 3, null, List.of("real big"), 4.3);
			indexRecord(i * uniqueDocs + 4, "something really special", List.of("small"), 1.6);
			indexRecord(i * uniqueDocs + 5, "something really special", List.of(), 4.1);
			indexRecord(i * uniqueDocs + 6, "boring and small", List.of(""), null);
		}

	}

	private void indexRecord(int id, String title, List<String> comments, Double rating) throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		String uniqueId = String.valueOf(id);

		Document mongoDocument = new Document();
		mongoDocument.put("id", uniqueId);
		mongoDocument.put("title", title);
		mongoDocument.put("comments", comments);
		mongoDocument.put("rating", rating);

		Store s = new Store(uniqueId, NUlL_TEST_INDEX);

		ResultDocBuilder resultDocumentBuilder = ResultDocBuilder.newBuilder().setDocument(mongoDocument);
		s.setResultDocument(resultDocumentBuilder);
		zuliaWorkPool.store(s);

	}

	@Test
	@Order(3)
	public void searchTest() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		//run the first search with realtime to force seeing latest changes without waiting for a commit
		Search search = new Search(NUlL_TEST_INDEX).setRealtime(true);
		SearchResult searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount * uniqueDocs, searchResult.getTotalHits());

		search = new Search(NUlL_TEST_INDEX);
		search.addQuery(new ScoredQuery("title:*")); // matches all documents that have titles that exist (matches empty as well)
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount * 6, searchResult.getTotalHits());

		search = new Search(NUlL_TEST_INDEX);
		search.addQuery(new ScoredQuery("-title:*")); // removes all documents that have titles that exist
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount, searchResult.getTotalHits());

		search = new Search(NUlL_TEST_INDEX);
		search.addQuery(new ScoredQuery("title:*?")); // matches all documents that have titles at least character long
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount * 5, searchResult.getTotalHits());

		search = new Search(NUlL_TEST_INDEX);
		search.addQuery(new ScoredQuery("-title:*?")); // removes all documents that have titles at least character long
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount * 2, searchResult.getTotalHits());

		search = new Search(NUlL_TEST_INDEX);
		search.addQuery(new ScoredQuery("|title|:0")); // null field is not returned here
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount, searchResult.getTotalHits());

		search = new Search(NUlL_TEST_INDEX);
		search.addQuery(new ScoredQuery("|comments|:0")); // null field is not returned here and empty list are not retuned
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount * 2, searchResult.getTotalHits());

		search = new Search(NUlL_TEST_INDEX);
		search.addQuery(new ScoredQuery("|||comments|||:0")); // matches the list of size 0 but not the list with one empty item or the null list
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount, searchResult.getTotalHits());

		search = new Search(NUlL_TEST_INDEX);
		search.addQuery(new ScoredQuery("comments:*")); // only the null valued would be excluded
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount * 6, searchResult.getTotalHits());

		search = new Search(NUlL_TEST_INDEX);
		search.addQuery(new ScoredQuery("|||comments|||:0")); // matches the list of size 0 but not the list with one empty item or the null list
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount, searchResult.getTotalHits());

		search = new Search(NUlL_TEST_INDEX);
		search.addQuery(new ScoredQuery("|||comments|||>2")); // only match documents with more than 2 comments
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount, searchResult.getTotalHits());

		search = new Search(NUlL_TEST_INDEX);
		search.addQuery(new ScoredQuery("|||comments|||:2")); // only match documents with exactly 2 comments (will match empty comments here)
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount, searchResult.getTotalHits());

		search = new Search(NUlL_TEST_INDEX);
		search.addQuery(new ScoredQuery("|comments|:0")); // match any documents with at least one comment of length 0 (empty string)
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount*2, searchResult.getTotalHits());


		search = new Search(NUlL_TEST_INDEX);
		//zulia rewrites pure negative queries (i.e. -title:boring -title:pink to -title:boring -title:pink *:*) to ensure they work out of the box
		//partial negative queries sometimes need an all docs match *:*
		search.addQuery(new ScoredQuery("(*:* AND -title:*) OR title:boring")).setDebug(true);
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(repeatCount*3, searchResult.getTotalHits());

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
