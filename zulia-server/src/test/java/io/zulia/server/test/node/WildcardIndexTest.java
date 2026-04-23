package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.Store;
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
public class WildcardIndexTest {

	@RegisterExtension
	static final NodeExtension nodeExtension = new NodeExtension(3);

	private static final List<String> LOG_INDEXES = List.of("logs-2026-04-19", "logs-2026-04-20", "logs-2026-04-21");
	private static final String METRICS_INDEX = "metrics-2026-04-21";

	private static final int DOCS_PER_LOG_INDEX = 10;
	private static final int DOCS_IN_METRICS = 4;

	@Test
	@Order(1)
	public void createIndexes() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		for (String name : LOG_INDEXES) {
			createIndex(zuliaWorkPool, name);
		}
		createIndex(zuliaWorkPool, METRICS_INDEX);
	}

	@Test
	@Order(2)
	public void indexDocs() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		for (String name : LOG_INDEXES) {
			for (int i = 0; i < DOCS_PER_LOG_INDEX; i++) {
				indexRecord(zuliaWorkPool, name, name + "-" + i, "log entry " + i);
			}
		}
		for (int i = 0; i < DOCS_IN_METRICS; i++) {
			indexRecord(zuliaWorkPool, METRICS_INDEX, METRICS_INDEX + "-" + i, "metric " + i);
		}
	}

	@Test
	@Order(3)
	public void wildcardMatchesAllLogs() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		SearchResult result = zuliaWorkPool.search(new Search("logs-*").setRealtime(true));
		Assertions.assertEquals(LOG_INDEXES.size() * DOCS_PER_LOG_INDEX, result.getTotalHits());
	}

	@Test
	@Order(4)
	public void questionMarkMatchesSingleChar() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		// logs-2026-04-2? matches logs-2026-04-20 and logs-2026-04-21, not logs-2026-04-19
		SearchResult result = zuliaWorkPool.search(new Search("logs-2026-04-2?").setRealtime(true));
		Assertions.assertEquals(2 * DOCS_PER_LOG_INDEX, result.getTotalHits());
	}

	@Test
	@Order(5)
	public void metricsWildcardMatchesOneIndex() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		SearchResult result = zuliaWorkPool.search(new Search("metrics-*").setRealtime(true));
		Assertions.assertEquals(DOCS_IN_METRICS, result.getTotalHits());
	}

	@Test
	@Order(6)
	public void literalAndWildcardMix() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		SearchResult result = zuliaWorkPool.search(new Search("logs-2026-04-19", "metrics-*").setRealtime(true));
		Assertions.assertEquals(DOCS_PER_LOG_INDEX + DOCS_IN_METRICS, result.getTotalHits());
	}

	@Test
	@Order(7)
	public void literalNameStillWorks() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		SearchResult result = zuliaWorkPool.search(new Search("logs-2026-04-20").setRealtime(true));
		Assertions.assertEquals(DOCS_PER_LOG_INDEX, result.getTotalHits());
	}

	@Test
	@Order(8)
	public void wildcardWithNoMatchesErrors() {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		Assertions.assertThrows(Exception.class, () -> zuliaWorkPool.search(new Search("nonexistent-*").setRealtime(true)),
				"wildcard matching no indexes should fail");
	}

	@Test
	@Order(9)
	public void wildcardDoesNotMatchAliases() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();
		zuliaWorkPool.createIndexAlias("everyLog", "logs-2026-04-21");
		// "every*" would match the alias "everyLog" if aliases were considered.
		// Expect no matches -> exception.
		Assertions.assertThrows(Exception.class, () -> zuliaWorkPool.search(new Search("every*").setRealtime(true)),
				"wildcard should only match real indexes, not aliases");
		// Sanity: the alias itself still resolves literally.
		SearchResult result = zuliaWorkPool.search(new Search("everyLog").setRealtime(true));
		Assertions.assertEquals(DOCS_PER_LOG_INDEX, result.getTotalHits());
	}

	private void createIndex(ZuliaWorkPool zuliaWorkPool, String indexName) throws Exception {
		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("message");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("message").indexAs(DefaultAnalyzers.STANDARD));
		indexConfig.setIndexName(indexName);
		indexConfig.setNumberOfShards(1);
		indexConfig.setShardCommitInterval(20);
		zuliaWorkPool.createIndex(indexConfig);
	}

	private void indexRecord(ZuliaWorkPool zuliaWorkPool, String index, String uniqueId, String message) throws Exception {
		Document mongoDocument = new Document();
		mongoDocument.put("id", uniqueId);
		mongoDocument.put("message", message);
		Store s = new Store(uniqueId, index);
		s.setResultDocument(ResultDocBuilder.newBuilder().setDocument(mongoDocument));
		zuliaWorkPool.store(s);
	}
}
