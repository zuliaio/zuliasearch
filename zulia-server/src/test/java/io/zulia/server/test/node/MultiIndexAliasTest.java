package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.DeleteFull;
import io.zulia.client.command.Fetch;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.FilterQuery;
import io.zulia.client.command.builder.Search;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.SearchResult;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.message.ZuliaIndex.IndexAlias;
import io.zulia.server.test.node.shared.NodeExtension;
import io.zulia.util.IndexAliasUtil;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class MultiIndexAliasTest {

	@RegisterExtension
	static final NodeExtension nodeExtension = new NodeExtension(3);

	private static final String INDEX_A = "multiAliasA";
	private static final String INDEX_B = "multiAliasB";
	private static final String INDEX_C = "multiAliasC";
	private static final String READ_ALIAS = "multiRead";
	private static final String WRITE_ALIAS = "multiWrite";
	private static final String ROLLOVER_ALIAS = "multiRollover";

	@Test
	@Order(1)
	public void createIndexes() throws Exception {
		ZuliaWorkPool pool = nodeExtension.getClient();
		for (String name : List.of(INDEX_A, INDEX_B, INDEX_C)) {
			ClientIndexConfig cfg = new ClientIndexConfig();
			cfg.addDefaultSearchField("title");
			cfg.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
			cfg.addFieldConfig(FieldConfigBuilder.createString("title").indexAs(DefaultAnalyzers.STANDARD).sort());
			cfg.setIndexName(name);
			cfg.setNumberOfShards(1);
			cfg.setShardCommitInterval(1);
			pool.createIndex(cfg);
		}
		indexDoc(INDEX_A, "a1", "apple");
		indexDoc(INDEX_A, "a2", "apricot");
		indexDoc(INDEX_B, "b1", "banana");
		indexDoc(INDEX_B, "b2", "blueberry");
		indexDoc(INDEX_B, "b3", "blackberry");
		indexDoc(INDEX_C, "c1", "cherry");
	}

	@Test
	@Order(2)
	public void readOnlyMultiIndexAlias() throws Exception {
		ZuliaWorkPool pool = nodeExtension.getClient();
		pool.createIndexAlias(READ_ALIAS, List.of(INDEX_A, INDEX_B, INDEX_C));

		SearchResult result = pool.search(new Search(READ_ALIAS).setRealtime(true));
		Assertions.assertEquals(6, result.getTotalHits(), "multi-index alias should fan out across all indexes");

		result = pool.search(new Search(INDEX_A, INDEX_B).setRealtime(true));
		Assertions.assertEquals(5, result.getTotalHits(), "explicit multi-index search baseline");
	}

	@Test
	@Order(3)
	public void singleOpsRejectedForReadOnlyMultiAlias() {
		ZuliaWorkPool pool = nodeExtension.getClient();

		Assertions.assertThrows(Exception.class, () -> indexDoc(READ_ALIAS, "x1", "nope"),
				"store should reject multi-index alias without writeIndex");
		Assertions.assertThrows(Exception.class, () -> pool.delete(new DeleteFull("a1", READ_ALIAS)),
				"delete should reject multi-index alias without writeIndex");
		Assertions.assertThrows(Exception.class, () -> pool.fetch(new Fetch("a1", READ_ALIAS)),
				"fetch should reject multi-index alias");
		Assertions.assertThrows(Exception.class, () -> pool.getNumberOfDocs(READ_ALIAS),
				"getNumberOfDocs should reject multi-index alias");
	}

	@Test
	@Order(4)
	public void writeIndexRoutesStoreAndDelete() throws Exception {
		ZuliaWorkPool pool = nodeExtension.getClient();
		pool.createIndexAlias(WRITE_ALIAS, List.of(INDEX_A, INDEX_B, INDEX_C), INDEX_B);

		Store store = new Store("w1", WRITE_ALIAS);
		store.setResultDocument(ResultDocBuilder.newBuilder().setDocument(new Document("id", "w1").append("title", "written via alias")));
		pool.store(store);

		SearchResult onB = pool.search(new Search(INDEX_B).setRealtime(true).addQuery(new FilterQuery("id:w1")));
		Assertions.assertEquals(1, onB.getTotalHits(), "document should land on designated writeIndex");

		SearchResult onA = pool.search(new Search(INDEX_A).setRealtime(true).addQuery(new FilterQuery("id:w1")));
		Assertions.assertEquals(0, onA.getTotalHits(), "document must not land on non-write members");

		SearchResult viaAlias = pool.search(new Search(WRITE_ALIAS).setRealtime(true).addQuery(new FilterQuery("id:w1")));
		Assertions.assertEquals(1, viaAlias.getTotalHits(), "fanned-out search through alias returns written doc");

		pool.delete(new DeleteFull("w1", WRITE_ALIAS));
		viaAlias = pool.search(new Search(WRITE_ALIAS).setRealtime(true).addQuery(new FilterQuery("id:w1")));
		Assertions.assertEquals(0, viaAlias.getTotalHits(), "delete through alias removed from writeIndex");
	}

	@Test
	@Order(5)
	public void writeIndexCanTargetNonReadMember() throws Exception {
		ZuliaWorkPool pool = nodeExtension.getClient();
		pool.createIndexAlias("backfill", List.of(INDEX_A, INDEX_B), INDEX_C);

		Store store = new Store("bf1", "backfill");
		store.setResultDocument(ResultDocBuilder.newBuilder().setDocument(new Document("id", "bf1").append("title", "backfilling")));
		pool.store(store);

		SearchResult onC = pool.search(new Search(INDEX_C).setRealtime(true).addQuery(new FilterQuery("id:bf1")));
		Assertions.assertEquals(1, onC.getTotalHits(), "writes land on writeIndex even when outside read members");

		SearchResult viaAlias = pool.search(new Search("backfill").setRealtime(true).addQuery(new FilterQuery("id:bf1")));
		Assertions.assertEquals(0, viaAlias.getTotalHits(), "reads only fan out to indexNames, not to writeIndex-outside-membership");
	}

	@Test
	@Order(6)
	public void backwardCompatSingleIndexAlias() throws Exception {
		ZuliaWorkPool pool = nodeExtension.getClient();
		pool.createIndexAlias("legacyAlias", INDEX_A);
		indexDoc("legacyAlias", "legacy1", "grape");
		SearchResult result = pool.search(new Search("legacyAlias").setRealtime(true));
		Assertions.assertTrue(result.getTotalHits() >= 3, "single-index alias still supports writes + reads");
	}

	@Test
	@Order(7)
	public void writeIndexFlipEnablesRollover() throws Exception {
		ZuliaWorkPool pool = nodeExtension.getClient();
		pool.createIndexAlias(ROLLOVER_ALIAS, List.of(INDEX_A, INDEX_B), INDEX_A);

		Store first = new Store("r1", ROLLOVER_ALIAS);
		first.setResultDocument(ResultDocBuilder.newBuilder().setDocument(new Document("id", "r1").append("title", "roll1")));
		pool.store(first);

		pool.createIndexAlias(ROLLOVER_ALIAS, List.of(INDEX_A, INDEX_B), INDEX_B);

		Store second = new Store("r2", ROLLOVER_ALIAS);
		second.setResultDocument(ResultDocBuilder.newBuilder().setDocument(new Document("id", "r2").append("title", "roll2")));
		pool.store(second);

		SearchResult onA = pool.search(new Search(INDEX_A).setRealtime(true).addQuery(new FilterQuery("id:r1 OR id:r2")));
		Assertions.assertEquals(1, onA.getTotalHits(), "pre-flip doc stays on old write target");

		SearchResult onB = pool.search(new Search(INDEX_B).setRealtime(true).addQuery(new FilterQuery("id:r1 OR id:r2")));
		Assertions.assertEquals(1, onB.getTotalHits(), "post-flip doc lands on new write target");

		SearchResult combined = pool.search(new Search(ROLLOVER_ALIAS).setRealtime(true).addQuery(new FilterQuery("id:r1 OR id:r2")));
		Assertions.assertEquals(2, combined.getTotalHits(), "alias still reads across both");
	}

	@Test
	@Order(8)
	public void nodesResponseExposesFullAlias() throws Exception {
		ZuliaWorkPool pool = nodeExtension.getClient();
		List<IndexAlias> aliases = pool.getNodes().getIndexAliases();
		IndexAlias write = aliases.stream().filter(a -> a.getAliasName().equals(WRITE_ALIAS)).findFirst().orElseThrow();
		Assertions.assertEquals(List.of(INDEX_A, INDEX_B, INDEX_C), IndexAliasUtil.getIndexNames(write));
		Assertions.assertEquals(INDEX_B, write.getWriteIndex());
	}

	@Test
	@Order(9)
	public void restartPreservesMultiIndexAlias() throws Exception {
		nodeExtension.restartNodes();
		ZuliaWorkPool pool = nodeExtension.getClient();
		SearchResult result = pool.search(new Search(READ_ALIAS).setRealtime(true));
		Assertions.assertTrue(result.getTotalHits() > 0, "alias membership survives restart");

		IndexAlias write = pool.getNodes().getIndexAliases().stream()
				.filter(a -> a.getAliasName().equals(WRITE_ALIAS)).findFirst().orElseThrow();
		Assertions.assertEquals(INDEX_B, write.getWriteIndex(), "writeIndex persists across restart");
	}

	private void indexDoc(String index, String id, String title) throws Exception {
		ZuliaWorkPool pool = nodeExtension.getClient();
		Store s = new Store(id, index);
		s.setResultDocument(ResultDocBuilder.newBuilder().setDocument(new Document("id", id).append("title", title)));
		pool.store(s);
	}
}
