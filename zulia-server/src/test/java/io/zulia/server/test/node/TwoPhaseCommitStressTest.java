package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.ZuliaFieldConstants;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.CountFacet;
import io.zulia.client.command.builder.Search;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.SearchResult;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.message.ZuliaQuery.FacetCount;
import io.zulia.server.search.aggregation.facets.BinaryFacetReader;
import io.zulia.server.search.aggregation.facets.FacetsReader;
import io.zulia.server.search.aggregation.ordinal.OrdinalConsumer;
import io.zulia.server.test.node.shared.NodeExtension;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.FSDirectory;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Stresses the taxonomy plus index commit pair under heavy concurrent stores and
 * verifies crash consistency the way a crash would: by repeatedly opening the last
 * committed index and taxonomy from disk while stores and commits race, and
 * checking that no committed document references a facet ordinal at or past the
 * committed taxonomy size. Every document creates a fresh taxonomy ordinal, so a
 * commit ordering that lets a store land between the taxonomy commit and the
 * index commit produces a detectable violation almost every commit cycle.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TwoPhaseCommitStressTest {

	// captured from the node's real config so the checker never drifts from the harness layout
	private static final java.util.concurrent.atomic.AtomicReference<Path> DATA_ROOT = new java.util.concurrent.atomic.AtomicReference<>();

	@RegisterExtension
	static final NodeExtension nodeExtension = new NodeExtension(1, config -> DATA_ROOT.set(Paths.get(config.getDataPath())));

	public static final String INDEX = "twoPhaseCommitStress";

	private static final int DOCS = 6000;
	private static final int BUCKETS = 10;
	private static final int WRITER_THREADS = 24;

	@Test
	@Order(1)
	public void createIndex() throws Exception {
		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.setIndexName(INDEX);
		indexConfig.setNumberOfShards(1);
		// every 20 docs so commits constantly overlap concurrent stores
		indexConfig.setShardCommitInterval(20);
		indexConfig.addDefaultSearchField("title");
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("title").indexAs(DefaultAnalyzers.STANDARD));
		// unique per document, forcing a brand new taxonomy ordinal on every store
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("category").indexAs(DefaultAnalyzers.KEYWORD).facet());
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("bucket").indexAs(DefaultAnalyzers.KEYWORD).facet());
		indexConfig.addFieldConfig(FieldConfigBuilder.createInt("num").index().sort());
		nodeExtension.getClient().createIndex(indexConfig);
	}

	@Test
	@Order(2)
	public void concurrentStoresWithCommittedStateChecker() throws Exception {
		ZuliaWorkPool workPool = nodeExtension.getClient();

		AtomicBoolean storing = new AtomicBoolean(true);
		List<String> violations = new CopyOnWriteArrayList<>();
		AtomicLong checksCompleted = new AtomicLong();

		Thread checker = Thread.ofPlatform().name("committed-state-checker").start(() -> {
			while (storing.get()) {
				try {
					// only passes that actually opened both commits count toward the
					// pressure guard, so a layout drift cannot degrade this into a no-op
					if (checkCommittedState(violations)) {
						checksCompleted.incrementAndGet();
					}
				}
				catch (IOException e) {
					// a commit can race the directory listing, retry on the next pass
				}
			}
		});

		try (ExecutorService executor = Executors.newFixedThreadPool(WRITER_THREADS)) {
			List<Future<?>> futures = new ArrayList<>();
			for (int t = 0; t < WRITER_THREADS; t++) {
				final int thread = t;
				futures.add(executor.submit(() -> {
					for (int i = thread; i < DOCS; i += WRITER_THREADS) {
						Document document = new Document();
						document.put("id", String.valueOf(i));
						document.put("title", "record number " + i);
						document.put("category", "cat" + i);
						document.put("bucket", "bucket" + (i % BUCKETS));
						document.put("num", i);
						workPool.store(new Store(String.valueOf(i), INDEX).setResultDocument(document));
					}
					return null;
				}));
			}
			for (Future<?> future : futures) {
				future.get();
			}
		}
		finally {
			storing.set(false);
			checker.join();
		}

		Assertions.assertTrue(checksCompleted.get() > 100, "checker only completed " + checksCompleted.get() + " passes, the test needs more pressure");
		Assertions.assertTrue(violations.isEmpty(), "committed index references ordinals missing from the committed taxonomy:\n" + String.join("\n", violations));
	}

	/**
	 * Opens the last committed index first and the taxonomy second. The taxonomy is
	 * then the same age or newer, so with a crash consistent commit ordering the
	 * committed taxonomy always covers every ordinal the committed index references,
	 * and this check can never fire.
	 */
	private boolean checkCommittedState(List<String> violations) throws IOException {
		Path indexDir = findShardDir("_0_idx");
		Path taxoDir = findShardDir("_0_facets");
		if (indexDir == null || taxoDir == null) {
			return false;
		}
		try (FSDirectory index = FSDirectory.open(indexDir); FSDirectory taxo = FSDirectory.open(taxoDir)) {
			if (!DirectoryReader.indexExists(index) || !DirectoryReader.indexExists(taxo)) {
				return false;
			}
			try (DirectoryReader indexReader = DirectoryReader.open(index); DirectoryTaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxo)) {
				// dimension ordinals are created with the first stored document, so once
				// present they identify the dims whose value ordinals the decoder surfaces
				int categoryOrdinal = taxoReader.getOrdinal(new FacetLabel("category"));
				int bucketOrdinal = taxoReader.getOrdinal(new FacetLabel("bucket"));
				if (categoryOrdinal < 0 || bucketOrdinal < 0) {
					return false;
				}
				int[] sortedDimOrdinals = { Math.min(categoryOrdinal, bucketOrdinal), Math.max(categoryOrdinal, bucketOrdinal) };
				int taxonomySize = taxoReader.getSize();
				int maxOrdinal = maxReferencedOrdinal(indexReader, sortedDimOrdinals);
				if (maxOrdinal >= taxonomySize) {
					violations.add("index commit gen " + indexReader.getIndexCommit().getGeneration() + " references ordinal " + maxOrdinal
							+ " but committed taxonomy size is " + taxonomySize);
				}
				return true;
			}
		}
	}

	/** Reads committed facet payloads through the production decoder rather than a hand rolled parser. */
	private static int maxReferencedOrdinal(DirectoryReader indexReader, int[] sortedDimOrdinals) throws IOException {
		MaxOrdinalConsumer consumer = new MaxOrdinalConsumer(sortedDimOrdinals);
		for (LeafReaderContext leaf : indexReader.leaves()) {
			FacetsReader facetsReader = new BinaryFacetReader(leaf.reader(), ZuliaFieldConstants.FACET_STORAGE, false);
			DocIdSetIterator docs = facetsReader.getCombinedIterator(DocIdSetIterator.all(leaf.reader().maxDoc()));
			for (int doc = docs.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = docs.nextDoc()) {
				facetsReader.getFacetHandler().handleFacets(consumer);
			}
		}
		return consumer.max();
	}

	private static final class MaxOrdinalConsumer implements OrdinalConsumer {

		private final int[] sortedDimOrdinals;
		private int max = -1;

		private MaxOrdinalConsumer(int[] sortedDimOrdinals) {
			this.sortedDimOrdinals = sortedDimOrdinals;
		}

		@Override
		public void handleOrdinal(int ordinal) {
			if (ordinal > max) {
				max = ordinal;
			}
		}

		@Override
		public int[] requestedDimensionOrdinals() {
			return sortedDimOrdinals;
		}

		private int max() {
			return max;
		}
	}

	private static Path findShardDir(String suffix) {
		Path dataRoot = DATA_ROOT.get();
		if (dataRoot == null) {
			return null;
		}
		Path shardDir = dataRoot.resolve("indexes").resolve(INDEX + suffix);
		return Files.isDirectory(shardDir) ? shardDir : null;
	}

	@Test
	@Order(3)
	public void realtimeCountsAndFacetsExact() throws Exception {
		ZuliaWorkPool workPool = nodeExtension.getClient();
		Search search = new Search(INDEX).setAmount(0).setRealtime(true);
		search.addCountFacet(new CountFacet("bucket").setTopN(BUCKETS));
		SearchResult result = workPool.search(search);
		Assertions.assertEquals(DOCS, result.getTotalHits());
		List<FacetCount> facetCounts = result.getFacetCounts("bucket");
		Assertions.assertEquals(BUCKETS, facetCounts.size());
		for (FacetCount facetCount : facetCounts) {
			Assertions.assertEquals(DOCS / BUCKETS, facetCount.getCount(), "uneven count for facet " + facetCount.getFacet());
		}
	}

	@Test
	@Order(4)
	public void committedViewCatchesUp() throws Exception {
		ZuliaWorkPool workPool = nodeExtension.getClient();
		workPool.optimizeIndex(INDEX);
		Search search = new Search(INDEX).setAmount(0);
		search.addCountFacet(new CountFacet("bucket").setTopN(BUCKETS));
		SearchResult result = workPool.search(search);
		Assertions.assertEquals(DOCS, result.getTotalHits());
		Assertions.assertEquals(BUCKETS, result.getFacetCounts("bucket").size());
	}
}
