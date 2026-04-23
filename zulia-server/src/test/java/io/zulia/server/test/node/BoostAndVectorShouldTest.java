package io.zulia.server.test.node;

import com.google.common.primitives.Floats;
import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.FilterQuery;
import io.zulia.client.command.builder.ScoredQuery;
import io.zulia.client.command.builder.Search;
import io.zulia.client.command.builder.VectorTopNQuery;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.CompleteResult;
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

import java.util.Set;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class BoostAndVectorShouldTest {

	@RegisterExtension
	static final NodeExtension nodeExtension = new NodeExtension(1);

	private static final String INDEX_NAME = "boostAndVectorShouldTest";

	// Four documents laid out so vector top-2 and BM25 matches partially overlap.
	// Query vector is [1,0,0,0]. Query text is "alpha".
	//   doc1: matches "alpha", vector close to query -> in BM25 AND vector top-2
	//   doc2: matches "alpha", vector close to query -> in BM25 AND vector top-2
	//   doc3: matches "alpha", vector far from query -> in BM25, NOT in vector top-2
	//   doc4: no "alpha",       vector far from query -> in neither
	private static final float[] V1 = { 1.0f, 0.0f, 0.0f, 0.0f };
	private static final float[] V2 = { 0.9f, 0.1f, 0.0f, 0.0f };
	private static final float[] V3 = { 0.0f, 0.0f, 1.0f, 0.0f };
	private static final float[] V4 = { 0.0f, 0.0f, 0.0f, 1.0f };
	private static final float[] QUERY_VECTOR = { 1.0f, 0.0f, 0.0f, 0.0f };

	@Test
	@Order(1)
	public void createIndex() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("title");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("title").indexAs(DefaultAnalyzers.STANDARD));
		indexConfig.addFieldConfig(FieldConfigBuilder.createVector("embedding").index());
		indexConfig.setIndexName(INDEX_NAME);
		indexConfig.setNumberOfShards(1);
		indexConfig.setShardCommitInterval(20);

		zuliaWorkPool.createIndex(indexConfig);
	}

	@Test
	@Order(2)
	public void indexDocuments() throws Exception {
		indexDoc("1", "alpha bravo", V1);
		indexDoc("2", "alpha charlie", V2);
		indexDoc("3", "alpha delta", V3);
		indexDoc("4", "echo", V4);
	}

	private void indexDoc(String id, String title, float[] vector) throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		Document doc = new Document();
		doc.put("id", id);
		doc.put("title", title);
		doc.put("embedding", Floats.asList(vector));
		zuliaWorkPool.store(new Store(id, INDEX_NAME, ResultDocBuilder.from(doc)));
	}

	@Test
	@Order(3)
	public void boostDoublesScore() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		SearchResult baseline = zuliaWorkPool.search(
				new Search(INDEX_NAME).setRealtime(true).setAmount(10).addQuery(new ScoredQuery("title:alpha")));
		Assertions.assertEquals(3, baseline.getTotalHits());

		SearchResult boosted = zuliaWorkPool.search(
				new Search(INDEX_NAME).setRealtime(true).setAmount(10).addQuery(new ScoredQuery("title:alpha").setBoost(2.0f)));
		Assertions.assertEquals(3, boosted.getTotalHits(), "boost must not change the matching doc set");

		// Matching doc set is identical; every doc's boosted score should be exactly 2x baseline.
		for (CompleteResult base : baseline.getCompleteResults()) {
			CompleteResult match = boosted.getCompleteResults().stream()
					.filter(r -> r.getUniqueId().equals(base.getUniqueId())).findFirst().orElseThrow();
			Assertions.assertEquals(base.getScore() * 2.0, match.getScore(), 1e-4,
					"doc " + base.getUniqueId() + " boost=2.0 should double the baseline score");
		}
	}

	@Test
	@Order(4)
	public void boostOfOneIsNoOp() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		SearchResult baseline = zuliaWorkPool.search(
				new Search(INDEX_NAME).setRealtime(true).setAmount(10).addQuery(new ScoredQuery("title:alpha")));

		SearchResult explicitOne = zuliaWorkPool.search(
				new Search(INDEX_NAME).setRealtime(true).setAmount(10).addQuery(new ScoredQuery("title:alpha").setBoost(1.0f)));

		// boost=1.0 should not wrap in BoostQuery, so scores are identical to no boost.
		Assertions.assertEquals(baseline.getTotalHits(), explicitOne.getTotalHits());
		for (CompleteResult base : baseline.getCompleteResults()) {
			CompleteResult match = explicitOne.getCompleteResults().stream()
					.filter(r -> r.getUniqueId().equals(base.getUniqueId())).findFirst().orElseThrow();
			Assertions.assertEquals(base.getScore(), match.getScore(), 1e-6,
					"boost=1.0 should be a no-op for doc " + base.getUniqueId());
		}
	}

	@Test
	@Order(5)
	public void boostReordersMixedShouldClauses() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Two SHOULD clauses match different docs. Boost one clause heavier to change which doc wins.
		// "bravo" matches only doc1. "charlie" matches only doc2. Both docs also match "alpha" equally.
		// With bravo boosted heavily, doc1 ranks above doc2; inverting the boost flips it.
		Search boostBravo = new Search(INDEX_NAME).setRealtime(true).setAmount(10)
				.addQuery(new ScoredQuery("title:alpha"))
				.addQuery(new ScoredQuery("title:bravo", false).setBoost(10.0f))
				.addQuery(new ScoredQuery("title:charlie", false).setBoost(1.0f));
		SearchResult bravoFirst = zuliaWorkPool.search(boostBravo);
		Assertions.assertEquals("1", bravoFirst.getFirstDocument().get("id"),
				"doc1 (bravo) should win when bravo is boosted 10x");

		Search boostCharlie = new Search(INDEX_NAME).setRealtime(true).setAmount(10)
				.addQuery(new ScoredQuery("title:alpha"))
				.addQuery(new ScoredQuery("title:bravo", false).setBoost(1.0f))
				.addQuery(new ScoredQuery("title:charlie", false).setBoost(10.0f));
		SearchResult charlieFirst = zuliaWorkPool.search(boostCharlie);
		Assertions.assertEquals("2", charlieFirst.getFirstDocument().get("id"),
				"doc2 (charlie) should win when charlie is boosted 10x");
	}

	@Test
	@Order(6)
	public void negativeBoostRejected() {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		Search search = new Search(INDEX_NAME).setAmount(10).addQuery(new ScoredQuery("title:alpha").setBoost(-1.0f));
		Assertions.assertThrows(Exception.class, () -> zuliaWorkPool.search(search),
				"negative boost should be rejected by the validator");
	}


	@Test
	@Order(7)
	public void vectorMustConstrainsResultSet() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// VECTOR (MUST) topN=2 constrains results to HNSW top-2 regardless of lexical SHOULD matches.
		// Only doc1 and doc2 are in vector top-2; doc3 matches "alpha" but is not in HNSW top-2.
		Search search = new Search(INDEX_NAME).setRealtime(true).setAmount(10)
				.addQuery(new VectorTopNQuery(QUERY_VECTOR, 2, "embedding"))
				.addQuery(new ScoredQuery("title:alpha", false));
		SearchResult result = zuliaWorkPool.search(search);

		Assertions.assertEquals(2, result.getTotalHits(),
				"VECTOR (MUST) topN=2 must cap results to 2 even when BM25 SHOULD matches more docs");
		Set<String> ids = Set.copyOf(result.getUniqueIds());
		Assertions.assertTrue(ids.contains("1") && ids.contains("2"),
				"vector top-2 should contain doc1 and doc2, got: " + ids);
		Assertions.assertFalse(ids.contains("3"),
				"doc3 matches BM25 but is outside vector top-2; VECTOR MUST must exclude it");
	}

	@Test
	@Order(8)
	public void vectorShouldDoesNotConstrainResultSet() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// Same shape, but VECTOR_SHOULD does not constrain; doc3 (BM25-only match) must appear.
		Search search = new Search(INDEX_NAME).setRealtime(true).setAmount(10)
				.addQuery(new VectorTopNQuery(QUERY_VECTOR, 2, "embedding").setMust(false))
				.addQuery(new ScoredQuery("title:alpha", false));
		SearchResult result = zuliaWorkPool.search(search);

		Assertions.assertEquals(3, result.getTotalHits(),
				"VECTOR_SHOULD topN=2 must not cap the result set; BM25 matches outside top-2 must be retained");
		Set<String> ids = Set.copyOf(result.getUniqueIds());
		Assertions.assertTrue(ids.containsAll(Set.of("1", "2", "3")),
				"expected doc1 + doc2 (vector top-2) and doc3 (BM25-only), got: " + ids);
		Assertions.assertFalse(ids.contains("4"),
				"doc4 matches neither retriever and must not appear");
	}

	@Test
	@Order(9)
	public void vectorShouldWithFilter() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// VECTOR_SHOULD alongside a real filter still respects the filter universe.
		Search search = new Search(INDEX_NAME).setRealtime(true).setAmount(10)
				.addQuery(new FilterQuery("title:alpha"))
				.addQuery(new VectorTopNQuery(QUERY_VECTOR, 2, "embedding").setMust(false));
		SearchResult result = zuliaWorkPool.search(search);

		Assertions.assertEquals(3, result.getTotalHits(),
				"filter restricts universe to alpha-matching docs; VECTOR_SHOULD must not widen it");
		Set<String> ids = Set.copyOf(result.getUniqueIds());
		Assertions.assertEquals(Set.of("1", "2", "3"), ids);
	}

	@Test
	@Order(10)
	public void vectorShouldContributesScoreToHybridRanking() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		// With VECTOR_SHOULD contributing score, doc1 (closest to query vector + BM25 match)
		// should outrank doc3 (BM25-only, vector far). doc3 still appears but ranks last.
		Search search = new Search(INDEX_NAME).setRealtime(true).setAmount(10)
				.addQuery(new VectorTopNQuery(QUERY_VECTOR, 4, "embedding").setMust(false))
				.addQuery(new ScoredQuery("title:alpha", false));
		SearchResult result = zuliaWorkPool.search(search);

		Assertions.assertTrue(result.getTotalHits() >= 3);
		String firstId = (String) result.getFirstDocument().get("id");
		Assertions.assertTrue(firstId.equals("1") || firstId.equals("2"),
				"a doc in vector top-2 that also matches BM25 should rank first, got: " + firstId);
	}
}
