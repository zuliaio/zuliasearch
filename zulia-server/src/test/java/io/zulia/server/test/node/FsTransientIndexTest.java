package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.Search;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.server.index.resident.LoadedIndexCache;
import io.zulia.server.test.node.shared.FsNodeExtension;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Transient index behavior in single-node filesystem mode: FSIndexService metadata reads happen on
 * every lazy load, so this suite also asserts that evict and reload churn does not grow the process
 * file descriptor count (the FSIndexService FileReader leak would grow it by two per reload).
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class FsTransientIndexTest {

	private static final String STABLE_INDEX = "fsStable";
	private static final String TRANSIENT_A = "fsTransientA";
	private static final String TRANSIENT_B = "fsTransientB";
	private static final int DOCS_PER_INDEX = 20;

	@RegisterExtension
	static final FsNodeExtension fsNode = new FsNodeExtension(zuliaConfig -> zuliaConfig.setTransientIndexCacheSize(1));

	private static LoadedIndexCache cache() {
		return fsNode.getNode().getIndexManager().getLoadedIndexCache();
	}

	@Test
	@Order(1)
	public void createAndIndex() throws Exception {
		ZuliaWorkPool zuliaWorkPool = fsNode.getClient();
		createIndex(zuliaWorkPool, STABLE_INDEX, false);
		createIndex(zuliaWorkPool, TRANSIENT_A, true);
		createIndex(zuliaWorkPool, TRANSIENT_B, true);

		for (String indexName : List.of(STABLE_INDEX, TRANSIENT_A, TRANSIENT_B)) {
			for (int i = 0; i < DOCS_PER_INDEX; i++) {
				indexRecord(zuliaWorkPool, indexName, indexName + "-" + i, "message " + i);
			}
			Assertions.assertEquals(DOCS_PER_INDEX, count(zuliaWorkPool, indexName));
		}
	}

	@Test
	@Order(2)
	public void evictionChurnDoesNotLeakFileDescriptors() throws Exception {
		Path fdDir = Path.of("/proc/self/fd");
		Assumptions.assumeTrue(Files.isDirectory(fdDir), "file descriptor check requires /proc (Linux)");

		ZuliaWorkPool zuliaWorkPool = fsNode.getClient();
		LoadedIndexCache cache = cache();

		// both transient indexes resident at measurement time so the baseline and end states match
		Assertions.assertEquals(DOCS_PER_INDEX, count(zuliaWorkPool, TRANSIENT_A));
		Assertions.assertEquals(DOCS_PER_INDEX, count(zuliaWorkPool, TRANSIENT_B));
		Thread.sleep(3000);
		long loadsBefore = cache.getLoadCount();
		long fdBefore = countFileDescriptors(fdDir);

		// with a cache bound of 1, alternating access keeps evicting the older index and reloading it,
		// and every reload reads settings and mapping from FSIndexService
		long end = System.currentTimeMillis() + 80_000;
		while (System.currentTimeMillis() < end) {
			Assertions.assertEquals(DOCS_PER_INDEX, count(zuliaWorkPool, TRANSIENT_A));
			Thread.sleep(2000);
			Assertions.assertEquals(DOCS_PER_INDEX, count(zuliaWorkPool, TRANSIENT_B));
			Thread.sleep(2000);
		}

		Assertions.assertEquals(DOCS_PER_INDEX, count(zuliaWorkPool, TRANSIENT_A));
		Assertions.assertEquals(DOCS_PER_INDEX, count(zuliaWorkPool, TRANSIENT_B));
		Thread.sleep(3000);
		long loadDelta = cache.getLoadCount() - loadsBefore;
		long fdGrowth = countFileDescriptors(fdDir) - fdBefore;

		Assertions.assertTrue(loadDelta >= 5, "expected evict and reload churn but only saw " + loadDelta + " loads");
		Assertions.assertTrue(fdGrowth < loadDelta,
				"file descriptors grew by " + fdGrowth + " over " + loadDelta + " reloads, which indicates a per-reload descriptor leak");
	}

	@Test
	@Order(3)
	public void lazyLoadAfterRestart() throws Exception {
		fsNode.restartNode();
		ZuliaWorkPool zuliaWorkPool = fsNode.getClient();

		List<String> resident = cache().getResidentIndexNames();
		Assertions.assertTrue(resident.contains(STABLE_INDEX), "non-transient index loads at startup in FS mode");
		Assertions.assertFalse(resident.contains(TRANSIENT_A), "transient index must not load at startup in FS mode");
		Assertions.assertFalse(resident.contains(TRANSIENT_B), "transient index must not load at startup in FS mode");

		Assertions.assertEquals(DOCS_PER_INDEX, count(zuliaWorkPool, TRANSIENT_A));
		Assertions.assertTrue(cache().getResidentIndexNames().contains(TRANSIENT_A));
	}

	@Test
	@Order(4)
	public void deleteUnloadedTransientIndexRemovesFsState() throws Exception {
		fsNode.restartNode();
		ZuliaWorkPool zuliaWorkPool = fsNode.getClient();
		Assertions.assertFalse(cache().getResidentIndexNames().contains(TRANSIENT_B), "transient index must not load at startup");

		zuliaWorkPool.deleteIndex(TRANSIENT_B);

		Path settingsFile = Path.of(fsNode.getDataPath(), "settings", TRANSIENT_B + "_index.json");
		Path shardDir = Path.of(fsNode.getDataPath(), "indexes", TRANSIENT_B + "_0_idx");
		Assertions.assertFalse(Files.exists(settingsFile), "deleting an unloaded transient index must remove its FS settings file");
		Assertions.assertFalse(Files.exists(shardDir), "deleting an unloaded transient index must remove its on-disk shard data");
		Assertions.assertFalse(zuliaWorkPool.getIndexes().getIndexNames().contains(TRANSIENT_B));

		Assertions.assertEquals(DOCS_PER_INDEX, count(zuliaWorkPool, STABLE_INDEX));
		Assertions.assertEquals(DOCS_PER_INDEX, count(zuliaWorkPool, TRANSIENT_A));
	}

	private long countFileDescriptors(Path fdDir) throws IOException {
		try (var fds = Files.list(fdDir)) {
			return fds.count();
		}
	}

	private long count(ZuliaWorkPool zuliaWorkPool, String indexName) throws Exception {
		return zuliaWorkPool.search(new Search(indexName).setAmount(0).setRealtime(true)).getTotalHits();
	}

	private void createIndex(ZuliaWorkPool zuliaWorkPool, String indexName, boolean transientIndex) throws Exception {
		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField("message");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.LC_KEYWORD).sort());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("message").indexAs(DefaultAnalyzers.STANDARD));
		indexConfig.setIndexName(indexName);
		indexConfig.setNumberOfShards(1);
		indexConfig.setShardCommitInterval(20);
		indexConfig.setTransientIndex(transientIndex);
		zuliaWorkPool.createIndex(indexConfig);
	}

	private void indexRecord(ZuliaWorkPool zuliaWorkPool, String index, String uniqueId, String message) throws Exception {
		Document mongoDocument = new Document();
		mongoDocument.put("id", uniqueId);
		mongoDocument.put("message", message);
		Store store = new Store(uniqueId, index);
		store.setResultDocument(ResultDocBuilder.newBuilder().setDocument(mongoDocument));
		zuliaWorkPool.store(store);
	}
}
