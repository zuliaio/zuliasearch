package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.CreateIndexAlias;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.ScoredQuery;
import io.zulia.client.command.builder.Search;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.message.ZuliaIndex.IndexAlias;
import io.zulia.server.test.node.shared.FsNodeExtension;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;

/**
 * Alias lifecycle in single-node filesystem mode. FSIndexService used to throw on a missing alias file where
 * MongoIndexService returns null, which made every alias creation fail on a single node.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class FsAliasTest {

	private static final String INDEX_A = "fsAliasA";
	private static final String INDEX_B = "fsAliasB";
	private static final String ALIAS = "fsAlias";

	@RegisterExtension
	static final FsNodeExtension fsNode = new FsNodeExtension();

	private static void createIndex(ZuliaWorkPool pool, String name) throws Exception {
		ClientIndexConfig config = new ClientIndexConfig();
		config.setIndexName(name);
		config.setNumberOfShards(1);
		config.addFieldConfig(FieldConfigBuilder.createString("title").indexAs(DefaultAnalyzers.STANDARD));
		pool.createIndex(config);
	}

	private static void store(ZuliaWorkPool pool, String index, String id) throws Exception {
		pool.store(new Store(id, index).setResultDocument(new Document("title", "hello " + id)));
	}

	private static long hits(ZuliaWorkPool pool, String indexOrAlias) throws Exception {
		Search search = new Search(indexOrAlias).setAmount(10).setRealtime(true);
		search.addQuery(new ScoredQuery("title:hello"));
		return pool.search(search).getTotalHits();
	}

	@Test
	@Order(1)
	public void createAliasOnSingleNode() throws Exception {
		ZuliaWorkPool pool = fsNode.getClient();
		createIndex(pool, INDEX_A);
		createIndex(pool, INDEX_B);
		store(pool, INDEX_A, "a1");
		store(pool, INDEX_B, "b1");

		pool.createIndexAlias(ALIAS, INDEX_A);

		Assertions.assertEquals(1, pool.getNodes().getIndexAliases().size());
		Assertions.assertEquals(1, hits(pool, ALIAS), "alias reads index A only");
	}

	@Test
	@Order(2)
	public void updateAliasToMultiIndexWithWriteIndex() throws Exception {
		ZuliaWorkPool pool = fsNode.getClient();
		IndexAlias alias = IndexAlias.newBuilder().setAliasName(ALIAS).addAllIndexNames(List.of(INDEX_A, INDEX_B)).setWriteIndex(INDEX_B).build();
		pool.createIndexAlias(new CreateIndexAlias(alias));

		Assertions.assertEquals(1, pool.getNodes().getIndexAliases().size(), "update replaces, it does not duplicate");
		Assertions.assertEquals(2, hits(pool, ALIAS), "alias now fans across both indexes");

		store(pool, ALIAS, "viaAlias");
		Assertions.assertEquals(2, hits(pool, INDEX_B), "writes through the alias land in the write index");
		Assertions.assertEquals(1, hits(pool, INDEX_A));
	}

	@Test
	@Order(3)
	public void deleteAndRecreateAlias() throws Exception {
		ZuliaWorkPool pool = fsNode.getClient();
		pool.deleteIndexAlias(ALIAS);
		Assertions.assertEquals(0, pool.getNodes().getIndexAliases().size());

		pool.createIndexAlias(ALIAS, INDEX_B);
		Assertions.assertEquals(2, hits(pool, ALIAS), "recreated alias points at index B alone");
	}

	@Test
	@Order(4)
	public void aliasSurvivesRestart() throws Exception {
		fsNode.restartNode();
		ZuliaWorkPool pool = fsNode.getClient();
		Assertions.assertEquals(1, pool.getNodes().getIndexAliases().size(), "alias file is read back after restart");
		Assertions.assertEquals(2, hits(pool, ALIAS));
	}
}
