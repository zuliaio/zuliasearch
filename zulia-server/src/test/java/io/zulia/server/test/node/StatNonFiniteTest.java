package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.NumericStat;
import io.zulia.client.command.builder.Search;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.message.ZuliaQuery.FacetStats;
import io.zulia.server.test.node.shared.NodeExtension;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;

/**
 * A NaN or infinite value in a sortable numeric field is stored and counted, but it must not fail a percentile request.
 * Percentiles cover the finite values only. Sum, min and max keep their plain double arithmetic.
 */
public class StatNonFiniteTest {

	@RegisterExtension
	static final NodeExtension nodeExtension = new NodeExtension(1);

	private static final String INDEX = "statNonFinite";

	@Test
	public void nonFiniteValuesDoNotBreakPercentiles() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createDouble("rating").index().sort());
		indexConfig.setIndexName(INDEX);
		indexConfig.setNumberOfShards(2);
		zuliaWorkPool.createIndex(indexConfig);

		store(zuliaWorkPool, "1", 1.0);
		store(zuliaWorkPool, "2", 2.0);
		store(zuliaWorkPool, "3", 3.0);
		store(zuliaWorkPool, "4", 4.0);
		store(zuliaWorkPool, "nan", Double.NaN);
		store(zuliaWorkPool, "inf", Double.POSITIVE_INFINITY);
		store(zuliaWorkPool, "negInf", Double.NEGATIVE_INFINITY);

		Search search = new Search(INDEX).setRealtime(true);
		search.addStat(new NumericStat("rating").setPercentiles(List.of(0.0, 0.5, 1.0)));
		FacetStats rating = zuliaWorkPool.search(search).getNumericFieldStat("rating");

		// every stored value is counted, the sketch holds the four finite ones
		Assertions.assertEquals(7L, rating.getValueCount());
		Assertions.assertEquals(7L, rating.getDocCount());
		Assertions.assertEquals(3, rating.getPercentilesCount());
		Assertions.assertEquals(1.0, rating.getPercentiles(0).getValue(), 0.001);
		Assertions.assertEquals(2.0, rating.getPercentiles(1).getValue(), 0.002);
		Assertions.assertEquals(4.0, rating.getPercentiles(2).getValue(), 0.004);

		// min and max are plain comparisons, so the infinities win and NaN is ignored, and the sum is NaN
		Assertions.assertEquals(Double.NEGATIVE_INFINITY, rating.getMin().getDoubleValue());
		Assertions.assertEquals(Double.POSITIVE_INFINITY, rating.getMax().getDoubleValue());
		Assertions.assertTrue(Double.isNaN(rating.getSum().getDoubleValue()));
	}

	private static void store(ZuliaWorkPool zuliaWorkPool, String id, double rating) throws Exception {
		Document document = new Document("id", id).append("rating", rating);
		zuliaWorkPool.store(new Store(id, INDEX).setResultDocument(ResultDocBuilder.from(document)));
	}
}
