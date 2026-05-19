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
import io.zulia.message.ZuliaBase.PrimaryReplicaSettings;
import io.zulia.server.test.node.shared.RestNodeExtension;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ReplicationTest {

	@RegisterExtension
	static final RestNodeExtension nodeExtension = new RestNodeExtension(2);

	private static final String INDEX = "replicationTest";
	private static final int DOC_COUNT = 50;

	private static final long REPLICATION_WAIT_MS = 30_000;
	private static final long REPLICATION_POLL_INTERVAL_MS = 200;

	@Test
	@Order(1)
	public void createIndexWithReplica() throws Exception {
		ZuliaWorkPool client = nodeExtension.getGrpcClient();

		ClientIndexConfig config = new ClientIndexConfig();
		config.addDefaultSearchField("title");
		config.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		config.addFieldConfig(FieldConfigBuilder.createString("title").indexAs(DefaultAnalyzers.STANDARD).sort());
		config.addFieldConfig(FieldConfigBuilder.createInt("seq").index().sort());
		config.setIndexName(INDEX);
		config.setNumberOfShards(1);
		config.setNumberOfReplicas(1);
		config.setShardCommitInterval(10); // commit every 10 docs to trigger replication

		client.createIndex(config);
	}

	@Test
	@Order(2)
	public void indexDocumentsAndReplicate() throws Exception {
		ZuliaWorkPool client = nodeExtension.getGrpcClient();

		for (int i = 0; i < DOC_COUNT; i++) {
			Document doc = new Document();
			doc.put("id", String.valueOf(i));
			doc.put("title", "doc " + i);
			doc.put("seq", i);
			Store store = new Store(String.valueOf(i), INDEX);
			store.setResultDocument(ResultDocBuilder.newBuilder().setDocument(doc));
			client.store(store);
		}

		// Optimize forces a final commit so the replica converges to a deterministic last generation
		// regardless of where the doc counter landed relative to shardCommitInterval.
		client.optimizeIndex(INDEX);

		long primaryHits = countHits(PrimaryReplicaSettings.PRIMARY_ONLY);
		Assertions.assertEquals(DOC_COUNT, primaryHits, "primary should have all docs after optimize");

		long replicaHits = waitForReplicaCount(DOC_COUNT);
		Assertions.assertEquals(DOC_COUNT, replicaHits, "replica did not converge to primary count within " + REPLICATION_WAIT_MS + "ms");
	}

	@Test
	@Order(3)
	public void primaryIfAvailableReturnsCorrectCount() throws Exception {
		long hits = countHits(PrimaryReplicaSettings.PRIMARY_IF_AVAILABLE);
		Assertions.assertEquals(DOC_COUNT, hits);
	}

	@Test
	@Order(4)
	public void additionalWritesContinueReplicating() throws Exception {
		ZuliaWorkPool client = nodeExtension.getGrpcClient();

		int extra = 25;
		for (int i = DOC_COUNT; i < DOC_COUNT + extra; i++) {
			Document doc = new Document();
			doc.put("id", String.valueOf(i));
			doc.put("title", "extra " + i);
			doc.put("seq", i);
			Store store = new Store(String.valueOf(i), INDEX);
			store.setResultDocument(ResultDocBuilder.newBuilder().setDocument(doc));
			client.store(store);
		}

		client.optimizeIndex(INDEX);

		int expected = DOC_COUNT + extra;
		long replicaHits = waitForReplicaCount(expected);
		Assertions.assertEquals(expected, replicaHits, "replica did not catch up after additional writes");
	}

	@Test
	@Order(5)
	public void fetchFromReplicaServesDocument() throws Exception {
		Search search = new Search(INDEX).setPrimaryReplicaSettings(PrimaryReplicaSettings.REPLICA_ONLY);
		search.addQuery(new ScoredQuery("seq:42"));
		SearchResult result = nodeExtension.getGrpcClient().search(search);
		Assertions.assertEquals(1, result.getTotalHits(), "replica should serve a doc indexed earlier in the suite");
	}

	@Test
	@Order(6)
	public void replicationStateEndpointReportsProgress() throws Exception {
		// Both nodes return the full shard mapping, but only the primary has lastAttemptedGeneration set.
		// Find the node that actually owns the primary by matching that field in the body.
		String primaryBody = fetchReplicationStateFromPrimary();
		Assertions.assertNotNull(primaryBody, "no REST node reported populated primary state");
		Assertions.assertTrue(primaryBody.contains("\"shardNumber\":0"), "expected shard 0 in response: " + primaryBody);
		Assertions.assertTrue(primaryBody.contains("lastAttemptedGeneration"), "expected lastAttemptedGeneration in response: " + primaryBody);
		Assertions.assertTrue(primaryBody.contains("\"replicas\""), "expected replicas array: " + primaryBody);
	}

	private long countHits(PrimaryReplicaSettings setting) throws Exception {
		Search search = new Search(INDEX).setPrimaryReplicaSettings(setting);
		search.setAmount(0);
		return nodeExtension.getGrpcClient().search(search).getTotalHits();
	}

	private long waitForReplicaCount(long expected) throws Exception {
		long deadline = System.currentTimeMillis() + REPLICATION_WAIT_MS;
		long replicaHits = -1;
		while (System.currentTimeMillis() < deadline) {
			replicaHits = countHits(PrimaryReplicaSettings.REPLICA_ONLY);
			if (replicaHits == expected) {
				return replicaHits;
			}
			Thread.sleep(REPLICATION_POLL_INTERVAL_MS);
		}
		return replicaHits;
	}

	private String fetchReplicationStateFromPrimary() throws Exception {
		HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build();
		// TestHelper assigns ports starting at 20001 (service) / 20002 (rest), incrementing per node.
		int[] restPorts = { 20002, 20004 };
		String fallback = null;
		for (int port : restPorts) {
			HttpRequest req = HttpRequest.newBuilder(URI.create("http://localhost:" + port + "/replication/" + INDEX)).timeout(Duration.ofSeconds(5)).GET()
					.build();
			HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
			if (resp.statusCode() != 200 || resp.body().isBlank()) {
				continue;
			}
			if (resp.body().contains("lastAttemptedGeneration")) {
				return resp.body();
			}
			fallback = resp.body();
		}
		return fallback;
	}
}
