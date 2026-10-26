package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.Search;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.SearchResult;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.message.ZuliaQuery.DistinctCount;
import io.zulia.server.test.node.shared.NodeExtension;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * A default distinct count must work however many shards a node answers for. The cap only bounds what a shard with many values
 * ships, so a field with a handful of values costs a handful of keys per shard and passes the internal message budget on any cluster.
 */
public class DistinctCountManyShardsTest {

	@RegisterExtension
	static final NodeExtension nodeExtension = new NodeExtension(3);

	private static final String INDEX = "distinctCountManyShards";
	// 90 shards over 3 nodes puts 30 on each, which the former cap based check refused for every default request
	private static final int SHARDS = 90;
	private static final int DOCS = 300;
	private static final int JOURNALS = 7;

	@Test
	public void defaultCountSucceedsWithThirtyShardsPerNode() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("journal").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createInt("year").index().sort());
		indexConfig.setIndexName(INDEX);
		indexConfig.setNumberOfShards(SHARDS);
		zuliaWorkPool.createIndex(indexConfig);

		for (int i = 0; i < DOCS; i++) {
			Document document = new Document("id", "doc" + i).append("journal", "JOURNAL" + (i % JOURNALS)).append("year", 2000 + (i % 13));
			zuliaWorkPool.store(new Store("doc" + i, INDEX).setResultDocument(ResultDocBuilder.from(document)));
		}

		Search search = new Search(INDEX).setAmount(0).setRealtime(true);
		search.addDistinctCount("journal");
		search.addDistinctCount("year");
		SearchResult result = zuliaWorkPool.search(search);

		Assertions.assertEquals(DOCS, result.getTotalHits());
		DistinctCount journals = result.getDistinctCount("journal");
		Assertions.assertEquals(JOURNALS, journals.getCount());
		Assertions.assertTrue(journals.getExact());
		Assertions.assertEquals(13, result.getDistinctCount("year").getCount());
	}
}
