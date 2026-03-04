package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.FilterQuery;
import io.zulia.client.command.builder.NumericStat;
import io.zulia.client.command.builder.Search;
import io.zulia.client.command.builder.StatFacet;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.SearchResult;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.message.ZuliaQuery.FacetStats;
import io.zulia.server.test.node.shared.NodeExtension;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;

/**
 * Reproduces the "gamma must be greater than 1" DDSketch exception.
 * <p>
 * The bug is in StatCombiner.convertAndCombineFacetStats() which only checks
 * the first shard's serializedSize to decide if results are empty. When the first
 * shard has data but a later shard has 0 hits, the later shard's globalStats is
 * FacetStatsInternal.getDefaultInstance() with no statSketch set. Deserializing
 * the default DDSketch proto produces gamma=0 which fails validation.
 * <p>
 * This also tests facet stats with documents that have a facet value but no
 * numeric value for the stat field — a separate scenario from the empty-shard case.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class StatFacetNoValueTest {

	@RegisterExtension
	static final NodeExtension nodeExtension = new NodeExtension(3);

	public static final String INDEX_NAME = "statFacetNoValue";

	// Use many shards with few docs to guarantee some shards have 0 matching documents
	private static final int SHARD_COUNT = 10;

	@Test
	@Order(1)
	public void createIndex() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("title");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("title").indexAs(DefaultAnalyzers.STANDARD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("category").indexAs(DefaultAnalyzers.LC_KEYWORD).facet().sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createDouble("price").index().sort());
		indexConfig.setIndexName(INDEX_NAME);
		indexConfig.setNumberOfShards(SHARD_COUNT);
		indexConfig.setShardCommitInterval(20);

		zuliaWorkPool.createIndex(indexConfig);
	}

	@Test
	@Order(2)
	public void index() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Index a small number of documents with price values and a "hasPrice" facet.
		// With 10 shards and only 3 documents, most shards will have 0 matching docs.
		for (int i = 0; i < 3; i++) {
			Document doc = new Document();
			doc.put("id", "hasPrice-" + i);
			doc.put("title", "item with price");
			doc.put("category", "hasPrice");
			doc.put("price", List.of(10.0 + i));
			Store s = new Store("hasPrice-" + i, INDEX_NAME);
			s.setResultDocument(ResultDocBuilder.from(doc));
			zuliaWorkPool.store(s);
		}

		// Documents with category "noPrice" but NO price value
		for (int i = 0; i < 3; i++) {
			Document doc = new Document();
			doc.put("id", "noPrice-" + i);
			doc.put("title", "item without price");
			doc.put("category", "noPrice");
			// price is intentionally omitted
			Store s = new Store("noPrice-" + i, INDEX_NAME);
			s.setResultDocument(ResultDocBuilder.from(doc));
			zuliaWorkPool.store(s);
		}
	}

	@Test
	@Order(3)
	public void numericStatWithPercentilesAndEmptyShards() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		List<Double> percentiles = List.of(0.0, 0.25, 0.50, 0.75, 1.0);

		// NumericStat (global stat) with percentiles.
		// With 10 shards and only 3 "hasPrice" docs, most shards have 0 hits.
		// Shards with 0 hits return FacetStatsInternal.getDefaultInstance() as globalStats
		// (no statSketch set). StatCombiner.convertAndCombineFacetStats only checks
		// getFirst().getSerializedSize() — if the first shard has data, it proceeds
		// to deserialize DDSketch from all shards, including default instances (gamma=0).
		Search search = new Search(INDEX_NAME).setRealtime(true);
		search.addStat(new NumericStat("price").setPercentiles(percentiles));
		search.addQuery(new FilterQuery("title:price"));

		SearchResult searchResult = zuliaWorkPool.search(search);

		FacetStats priceStat = searchResult.getNumericFieldStat("price");
		Assertions.assertEquals(3, priceStat.getDocCount());
		Assertions.assertEquals(3, priceStat.getValueCount());
		Assertions.assertEquals(10.0, priceStat.getMin().getDoubleValue(), 0.01);
		Assertions.assertEquals(12.0, priceStat.getMax().getDoubleValue(), 0.01);
	}

	@Test
	@Order(4)
	public void statFacetWithPercentilesOnMissingNumericValues() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		List<Double> percentiles = List.of(0.0, 0.25, 0.50, 0.75, 1.0);

		// StatFacet with percentiles where some facet values have no numeric values.
		// The "noPrice" facet has documents but none have a price value.
		Search search = new Search(INDEX_NAME).setRealtime(true);
		search.addStat(new StatFacet("price", "category").setPercentiles(percentiles));

		SearchResult searchResult = zuliaWorkPool.search(search);

		List<FacetStats> priceByCategory = searchResult.getFacetFieldStat("price", "category");
		Assertions.assertFalse(priceByCategory.isEmpty(), "Should have facet stats");

		for (FacetStats facetStats : priceByCategory) {
			if (facetStats.getFacet().equals("hasPrice")) {
				Assertions.assertEquals(3, facetStats.getDocCount());
				Assertions.assertEquals(3, facetStats.getValueCount());
				Assertions.assertTrue(facetStats.getMin().getDoubleValue() >= 10.0);
				Assertions.assertTrue(facetStats.getMax().getDoubleValue() <= 12.0);
			}
			else if (facetStats.getFacet().equals("noPrice")) {
				// These docs have the facet but no numeric value
				Assertions.assertEquals(0, facetStats.getDocCount());
				Assertions.assertEquals(3, facetStats.getAllDocCount());
				Assertions.assertEquals(0, facetStats.getValueCount());
				Assertions.assertEquals(0, facetStats.getPercentilesCount(),
						"No percentiles should be returned for facet with no values");
			}
			else {
				Assertions.fail("Unexpected facet: " + facetStats.getFacet());
			}
		}
	}
}
