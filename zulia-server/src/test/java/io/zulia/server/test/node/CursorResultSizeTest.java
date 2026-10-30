package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.ZuliaFieldConstants;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.Highlight;
import io.zulia.client.command.builder.ScoredQuery;
import io.zulia.client.command.builder.Search;
import io.zulia.client.command.builder.Sort;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.CompleteResult;
import io.zulia.client.result.SearchResult;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.message.ZuliaBase.CacheStats;
import io.zulia.message.ZuliaBase.IndexStats;
import io.zulia.server.test.node.shared.RestNodeExtension;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.List;

/**
 * The next cursor of a shard response carries only the score and sort values, so a cached page costs the documents on the page and not one more.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class CursorResultSizeTest {

	@RegisterExtension
	static final RestNodeExtension nodeExtension = new RestNodeExtension(1); // stats are read over REST

	private static final String INDEX = "cursorResultSize";
	private static final int DOCS = 6;
	private static final int BODY_BYTES = 400 * 1024;
	private static final String BODY = "b".repeat(BODY_BYTES);

	@Test
	@Order(1)
	public void createIndex() throws Exception {
		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("title");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("title").indexAs(DefaultAnalyzers.STANDARD));
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("body"));
		indexConfig.setIndexName(INDEX);
		indexConfig.setNumberOfShards(1);
		nodeExtension.getGrpcClient().createIndex(indexConfig);
	}

	@Test
	@Order(2)
	public void index() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getGrpcClient();
		for (int i = 0; i < DOCS; i++) {
			Document document = new Document().append("id", "d" + i).append("title", "publication " + i).append("body", BODY);
			zuliaWorkPool.store(new Store("d" + i, INDEX).setResultDocument(ResultDocBuilder.newBuilder().setDocument(document)));
		}
		Assertions.assertEquals(DOCS, zuliaWorkPool.search(new Search(INDEX).setAmount(0).setRealtime(true)).getTotalHits());
	}

	@Test
	@Order(3)
	public void cachedPageHoldsOneDocumentAndPagingStillWorks() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getGrpcClient();

		Search page = new Search(INDEX).setAmount(1).addQuery(new ScoredQuery("title:publication")).addSort(new Sort("id")).addHighlight(new Highlight("title"));
		SearchResult first = zuliaWorkPool.search(page);
		Assertions.assertEquals(DOCS, first.getTotalHits());
		Assertions.assertEquals(1, first.getCompleteResults().size());
		Assertions.assertEquals(1, zuliaWorkPool.search(page).getShardsCached(), "the first page is cached");

		CompleteResult hit = first.getFirstCompleteResult();
		Assertions.assertEquals("d0", hit.getUniqueId());
		Assertions.assertEquals(BODY_BYTES, hit.getDocument().getString("body").length(), "the page hit still carries its full document");
		Assertions.assertFalse(hit.getHighlightsForField("title").isEmpty(), "the page hit still carries its highlight");

		CacheStats general = zuliaWorkPool.getStats().getNodeStats().getIndexStatList().stream().filter(s -> s.getIndexName().equals(INDEX)).findFirst()
				.map(IndexStats::getShardCacheStatList).orElseThrow().getFirst().getGeneralCache();
		Assertions.assertTrue(general.getResultSize() > BODY_BYTES, "the cached page holds the hit's document: " + general);
		Assertions.assertTrue(general.getResultSize() < 2 * BODY_BYTES, "the cached page does not hold the cursor's document as well: " + general);

		List<String> ids = new ArrayList<>();
		Search paging = new Search(INDEX).setAmount(1).addQuery(new ScoredQuery("title:publication")).addSort(new Sort("id"));
		zuliaWorkPool.searchAll(paging, sr -> {
			for (CompleteResult completeResult : sr.getCompleteResults()) {
				ids.add(completeResult.getUniqueId());
				Assertions.assertEquals(BODY_BYTES, completeResult.getDocument().getString("body").length());
			}
		});
		Assertions.assertEquals(List.of("d0", "d1", "d2", "d3", "d4", "d5"), ids, "the cursor built without the document still pages in sort order");

		List<String> scoreOrder = new ArrayList<>();
		zuliaWorkPool.searchAll(new Search(INDEX).setAmount(2).addQuery(new ScoredQuery("title:publication")).addSort(new Sort(ZuliaFieldConstants.ID_SORT_FIELD)),
				sr -> sr.getCompleteResults().forEach(r -> scoreOrder.add(r.getUniqueId())));
		Assertions.assertEquals(ids, scoreOrder, "paging on the id sort field reaches every document once");
	}
}
