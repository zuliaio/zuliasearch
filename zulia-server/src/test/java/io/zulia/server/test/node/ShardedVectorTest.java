package io.zulia.server.test.node;

import com.google.common.primitives.Floats;
import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.Search;
import io.zulia.client.command.builder.VectorTopNQuery;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.SearchResult;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.server.test.node.shared.NodeExtension;
import org.bson.Document;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.HashSet;
import java.util.Random;
import java.util.Set;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ShardedVectorTest {

	@RegisterExtension
	static final NodeExtension nodeExtension = new NodeExtension(3);

	public static final String SHARDED_VECTOR_TEST = "shardedVectorTest";

	private static final int SHARD_COUNT = 5;
	private static final int DOC_COUNT = 100;
	private static final int VECTOR_DIM = 4;

	@Test
	@Order(1)
	public void createIndex() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("title");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("title").indexAs(DefaultAnalyzers.STANDARD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createVector("v").index());
		indexConfig.setIndexName(SHARDED_VECTOR_TEST);
		indexConfig.setNumberOfShards(SHARD_COUNT);
		indexConfig.setShardCommitInterval(20);

		zuliaWorkPool.createIndex(indexConfig);
	}

	@Test
	@Order(2)
	public void index() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		Random random = new Random(42);

		for (int i = 0; i < DOC_COUNT; i++) {
			String uniqueId = String.valueOf(i);

			float[] vector = createRandomVector(random);

			Document mongoDocument = new Document();
			mongoDocument.put("id", uniqueId);
			mongoDocument.put("title", "Document " + i);
			mongoDocument.put("v", Floats.asList(vector));

			Store store = new Store(uniqueId, SHARDED_VECTOR_TEST);
			store.setResultDocument(ResultDocBuilder.newBuilder().setDocument(mongoDocument));
			zuliaWorkPool.store(store);
		}
	}

	private static float @NonNull [] createRandomVector(Random random) {
		float[] vector = new float[VECTOR_DIM];
		float norm = 0;
		for (int d = 0; d < VECTOR_DIM; d++) {
			vector[d] = random.nextFloat();
			norm += vector[d] * vector[d];
		}
		// normalize to unit vector
		norm = (float) Math.sqrt(norm);
		for (int d = 0; d < VECTOR_DIM; d++) {
			vector[d] /= norm;
		}
		return vector;
	}

	@Test
	@Order(3)
	public void searchTest() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		float[] queryVector = { 1.0f, 0.0f, 0.0f, 0.0f };

		// force seeing latest changes
		Search search = new Search(SHARDED_VECTOR_TEST).setRealtime(true);
		SearchResult searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(DOC_COUNT, searchResult.getTotalHits());

		// vectorTopN=3 across 5 shards should return exactly 3 results, not 5*3=15
		int topN = 3;
		search = new Search(SHARDED_VECTOR_TEST);
		search.addQuery(new VectorTopNQuery(queryVector, topN, "v"));
		search.setAmount(DOC_COUNT);
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(topN, searchResult.getTotalHits(),
				"vectorTopN=" + topN + " with " + SHARD_COUNT + " shards should return " + topN + " total results, not " + SHARD_COUNT + "*" + topN);
		Assertions.assertEquals(topN, searchResult.getCompleteResults().size());

		// vectorTopN=10 across 5 shards should return exactly 10 results, not 50
		topN = 10;
		search = new Search(SHARDED_VECTOR_TEST);
		search.addQuery(new VectorTopNQuery(queryVector, topN, "v"));
		search.setAmount(DOC_COUNT);
		searchResult = zuliaWorkPool.search(search);
		Assertions.assertEquals(topN, searchResult.getTotalHits(),
				"vectorTopN=" + topN + " with " + SHARD_COUNT + " shards should return " + topN + " total results, not " + SHARD_COUNT + "*" + topN);
		Assertions.assertEquals(topN, searchResult.getCompleteResults().size());

		// verify results are sorted by score descending (best matches first)
		double previousScore = Double.MAX_VALUE;
		for (int i = 0; i < searchResult.getCompleteResults().size(); i++) {
			double score = searchResult.getCompleteResults().get(i).getScore();
			Assertions.assertTrue(score <= previousScore,
					"Results should be sorted by score descending: " + previousScore + " should be >= " + score);
			previousScore = score;
		}

		// verify no duplicate results across shard merging
		Set<String> uniqueIds = new HashSet<>();
		for (var result : searchResult.getCompleteResults()) {
			Assertions.assertTrue(uniqueIds.add(result.getUniqueId()), "Duplicate result found: " + result.getUniqueId());
		}

		// vectorTopN=3 results should be a subset of vectorTopN=10 results
		search = new Search(SHARDED_VECTOR_TEST);
		search.addQuery(new VectorTopNQuery(queryVector, 3, "v"));
		search.setAmount(DOC_COUNT);
		SearchResult top3Result = zuliaWorkPool.search(search);

		search = new Search(SHARDED_VECTOR_TEST);
		search.addQuery(new VectorTopNQuery(queryVector, 10, "v"));
		search.setAmount(DOC_COUNT);
		SearchResult top10Result = zuliaWorkPool.search(search);

		Set<String> top10Ids = new HashSet<>();
		for (var result : top10Result.getCompleteResults()) {
			top10Ids.add(result.getUniqueId());
		}
		for (var result : top3Result.getCompleteResults()) {
			Assertions.assertTrue(top10Ids.contains(result.getUniqueId()),
					"Top 3 result " + result.getUniqueId() + " should also appear in top 10 results");
		}
	}

	@Test
	@Order(4)
	public void restart() throws Exception {
		nodeExtension.restartNodes();
	}

	@Test
	@Order(5)
	public void confirm() throws Exception {
		searchTest();
	}

}
