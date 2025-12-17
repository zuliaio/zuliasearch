package io.zulia.server.index;

import io.zulia.ZuliaFieldConstants;
import io.zulia.server.analysis.ZuliaPerFieldAnalyzer;
import io.zulia.server.config.ServerIndexConfig;
import io.zulia.server.index.cache.ZuliaTaxonomyWriterCache;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.facet.taxonomy.writercache.LruTaxonomyWriterCache;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NRTCachingDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;

public class ShardWriteManager {

	private final static Logger LOG = LoggerFactory.getLogger(ShardWriteManager.class);

	private final ZuliaPerFieldAnalyzer zuliaPerFieldAnalyzer;
	private final ShardDocumentIndexer shardDocumentIndexer;
	private final ServerIndexConfig indexConfig;
	private final int shardNumber;
	private final String indexName;
	private final AtomicLong counter;
	private final ZuliaConcurrentMergeScheduler mergeScheduler;
	private final Semaphore indexingThrottle;

	private final IndexWriter indexWriter;
	private final DirectoryTaxonomyWriter taxoWriter;

	private Long lastCommit;
	private Long lastChange;
	private Long lastWarm;

	private final AtomicLong totalIndexedUnthrottled;
	private final AtomicLong totalIndexedThrottled;

	public ShardWriteManager(int shardNumber, Path pathToIndex, Path pathToTaxoIndex, ServerIndexConfig indexConfig,
			ZuliaPerFieldAnalyzer zuliaPerFieldAnalyzer) throws IOException {

		this.shardNumber = shardNumber;
		this.zuliaPerFieldAnalyzer = zuliaPerFieldAnalyzer;
		this.indexConfig = indexConfig;
		this.indexName = indexConfig.getIndexName();

		this.shardDocumentIndexer = new ShardDocumentIndexer(indexConfig);

		this.counter = new AtomicLong();
		this.lastCommit = null;
		this.lastChange = null;
		this.lastWarm = null;

		this.totalIndexedUnthrottled = new AtomicLong();
		this.totalIndexedThrottled = new AtomicLong();
		this.indexingThrottle = new Semaphore(1);
		this.mergeScheduler = new ZuliaConcurrentMergeScheduler();

		this.indexWriter = openIndexWriter(pathToIndex);
		this.taxoWriter = openTaxoWriter(pathToTaxoIndex);

		updateIndexSettings();

	}

	public int getShardNumber() {
		return shardNumber;
	}

	public ServerIndexConfig getIndexConfig() {
		return indexConfig;
	}

	private IndexWriter openIndexWriter(Path pathToIndex) throws IOException {

		Directory d = MMapDirectory.open(pathToIndex);

		IndexWriterConfig config = new IndexWriterConfig(zuliaPerFieldAnalyzer);
		config.setIndexDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
		config.setMaxBufferedDocs(Integer.MAX_VALUE);
		config.setRAMBufferSizeMB(128); // should be overwritten by ZuliaShard.updateIndexSettings()
		config.setUseCompoundFile(false);

		config.setMergeScheduler(mergeScheduler);

		NRTCachingDirectory nrtCachingDirectory = new NRTCachingDirectory(d, 50, 150);

		return new IndexWriter(nrtCachingDirectory, config);

	}

	private DirectoryTaxonomyWriter openTaxoWriter(Path pathToTaxo) throws IOException {
		Directory d = MMapDirectory.open(pathToTaxo);
		NRTCachingDirectory nrtCachingDirectory = new NRTCachingDirectory(d, 5, 15);
		return new DirectoryTaxonomyWriter(nrtCachingDirectory, IndexWriterConfig.OpenMode.CREATE_OR_APPEND, new ZuliaTaxonomyWriterCache());
	}

	public void close() throws IOException {
		{
			Directory directory = indexWriter.getDirectory();
			indexWriter.close();
			directory.close();
		}
		{
			Directory directory = taxoWriter.getDirectory();
			taxoWriter.close();
			directory.close();

		}
	}

	public ShardReader createShardReader() throws IOException {
		DirectoryReader indexReader = DirectoryReader.open(indexWriter);
		DirectoryTaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoWriter);
		taxoReader.setCacheSize(128000);
		return new ShardReader(shardNumber, indexReader, taxoReader, indexConfig, zuliaPerFieldAnalyzer);
	}

	public void commit() throws IOException {
		long indexedThrottled = totalIndexedThrottled.get();
		long indexedUntrottled = totalIndexedUnthrottled.get();
		LOG.info("Committing index {}:s{} with {} total indexed ({} indexed throttled)", indexName, shardNumber, indexedUntrottled + indexedThrottled,
				indexedThrottled);

		long currentTime = System.currentTimeMillis();
		indexWriter.commit();
		taxoWriter.commit();

		lastCommit = currentTime;
	}

	public boolean needsIdleCommit() {
		long currentTime = System.currentTimeMillis();

		long msIdleWithoutCommit = indexConfig.getIndexSettings().getIdleTimeWithoutCommit() * 1000L;

		//reassign so it can't change in the middle of the logic
		Long lastChange = this.lastChange;
		Long lastCommit = this.lastCommit;

		if (lastChange != null) { // if there has been a change
			long timeSinceLastChange = currentTime - lastChange;
			if (timeSinceLastChange > msIdleWithoutCommit) {
				//if there has never been a commit or the last change is after the last commit
				return (lastCommit == null) || (lastChange > lastCommit);
			}
		}
		return false;
	}

	WarmInfo needsSearchWarming(long lastestShardTime) {
		long currentTime = System.currentTimeMillis();

		//reassign so it can't change in the middle of the logic
		Long lastChange = this.lastChange;
		Long lastCommit = this.lastCommit;
		Long lastWarm = this.lastWarm;

		if (lastWarm == null) {
			// never warmed so needs warmed and the index is idle
			boolean idle = lastChange == null || (Math.abs(currentTime - lastChange) > 1000L && (lastCommit == null || (lastCommit > lastChange)));
			return new WarmInfo(idle, lastChange, lastCommit);
		}

		if (lastCommit != null && lastChange != null) { // if there has been a change to the index and a commit
			if (lastChange < lastCommit) { // no changes since last commit
				if (lastWarm < lastestShardTime) { // Change is committed AND shard reader has been reopened
					return new WarmInfo(true, lastChange, lastCommit);
				}
			}
		}

		return new WarmInfo(false, lastChange, lastCommit);
	}

	public Long getLastChanged() {
		return lastChange;
	}

	public Long getLastCommit() {
		return lastCommit;
	}

	public Long getLastWarm() {
		return lastWarm;
	}

	public void searchesWarmed(long time) {
		lastWarm = time;
	}

	public boolean markedChangedCheckIfCommitNeeded() {
		lastChange = System.currentTimeMillis();

		long count = counter.incrementAndGet();
		return (count % indexConfig.getIndexSettings().getShardCommitInterval()) == 0;

	}

	public void updateIndexSettings() {
		int ramBufferMB = indexConfig.getRAMBufferMB() != 0 ? indexConfig.getRAMBufferMB() : 128;
		indexWriter.getConfig().setRAMBufferSizeMB(ramBufferMB);
		lastWarm = null;
	}

	public void deleteDocuments(String uniqueId) throws IOException {
		Term term = new Term(ZuliaFieldConstants.ID_FIELD, uniqueId);
		indexWriter.deleteDocuments(term);
	}

	public void forceMerge(int maxNumberSegments) throws IOException {
		indexWriter.forceMerge(maxNumberSegments);

	}

	public void deleteAll() throws IOException {
		indexWriter.deleteAll();
	}

	public void indexDocument(String uniqueId, long timestamp, DocumentContainer mongoDocument, DocumentContainer metadata) throws Exception {
		Document luceneDocument = shardDocumentIndexer.getIndexDocument(uniqueId, timestamp, mongoDocument, metadata, taxoWriter);
		Term updateQuery = new Term(ZuliaFieldConstants.ID_FIELD, uniqueId);

		if (mergeScheduler.mergeSaturated()) {
			indexingThrottle.acquire();
			try {
				indexWriter.updateDocument(updateQuery, luceneDocument);
				totalIndexedThrottled.incrementAndGet();
			}
			finally {
				indexingThrottle.release();
			}
		}
		else {
			indexWriter.updateDocument(updateQuery, luceneDocument);
			totalIndexedUnthrottled.incrementAndGet();
		}

	}

}
