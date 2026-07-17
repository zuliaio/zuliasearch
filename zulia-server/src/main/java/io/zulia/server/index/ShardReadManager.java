package io.zulia.server.index;

import io.zulia.server.analysis.ZuliaPerFieldAnalyzer;
import io.zulia.server.config.ServerIndexConfig;
import io.zulia.server.search.aggregation.AggregationSettings;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ShardReadManager implements Closeable {

	private final static Logger LOG = LoggerFactory.getLogger(ShardReadManager.class);

	private final int shardNumber;
	private final Path pathToIndex;
	private final Path pathToTaxoIndex;
	private final ServerIndexConfig indexConfig;
	private final ZuliaPerFieldAnalyzer zuliaPerFieldAnalyzer;
	private final AggregationSettings aggregationSettings;
	private final ExecutorService segmentOpenExecutor;

	private final Directory indexDirectory;
	private final Directory taxoDirectory;

	public ShardReadManager(int shardNumber, Path pathToIndex, Path pathToTaxoIndex, ServerIndexConfig indexConfig,
			ZuliaPerFieldAnalyzer zuliaPerFieldAnalyzer, AggregationSettings aggregationSettings) throws IOException {
		this.shardNumber = shardNumber;
		this.pathToIndex = pathToIndex;
		this.pathToTaxoIndex = pathToTaxoIndex;
		this.indexConfig = indexConfig;
		this.zuliaPerFieldAnalyzer = zuliaPerFieldAnalyzer;
		this.aggregationSettings = aggregationSettings;

		String indexName = indexConfig.getIndexName();
		this.segmentOpenExecutor = Executors.newThreadPerTaskExecutor(
				Thread.ofVirtual().name(indexName + ":s" + shardNumber + "-replica-segment-", 0).factory());

		Files.createDirectories(pathToIndex);
		Files.createDirectories(pathToTaxoIndex);

		this.indexDirectory = MMapDirectory.open(pathToIndex);
		this.taxoDirectory = MMapDirectory.open(pathToTaxoIndex);

		ensureIndexExists(indexDirectory);
		ensureTaxoIndexExists(taxoDirectory);
	}

	private void ensureIndexExists(Directory directory) throws IOException {
		if (!DirectoryReader.indexExists(directory)) {
			LOG.info("Creating empty index for replica shard {}", shardNumber);
			try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
				writer.commit();
			}
		}
	}

	private void ensureTaxoIndexExists(Directory directory) throws IOException {
		if (!DirectoryReader.indexExists(directory)) {
			LOG.info("Creating empty taxonomy index for replica shard {}", shardNumber);
			try (DirectoryTaxonomyWriter writer = new DirectoryTaxonomyWriter(directory)) {
				writer.commit();
			}
		}
	}

	public int getShardNumber() {
		return shardNumber;
	}

	public ServerIndexConfig getIndexConfig() {
		return indexConfig;
	}

	public ZuliaPerFieldAnalyzer getZuliaPerFieldAnalyzer() {
		return zuliaPerFieldAnalyzer;
	}

	public Directory getIndexDirectory() {
		return indexDirectory;
	}

	public Directory getTaxoDirectory() {
		return taxoDirectory;
	}

	public Path getPathToIndex() {
		return pathToIndex;
	}

	public Path getPathToTaxoIndex() {
		return pathToTaxoIndex;
	}

	// Safe after refreshReaders; mmapped files held by in-flight queries stay valid (mmap-after-unlink).
	public void cleanupUnreferencedFiles() {
		cleanupDirectory(indexDirectory, "index");
		cleanupDirectory(taxoDirectory, "taxonomy");
	}

	private void cleanupDirectory(Directory directory, String label) {
		Set<String> referenced;
		try {
			SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(directory);
			referenced = new HashSet<>(segmentInfos.files(true));
		}
		catch (IOException e) {
			LOG.warn("Skipping {} cleanup for replica shard {}: could not read segment infos: {}", label, shardNumber, e.getMessage());
			return;
		}

		String[] allFiles;
		try {
			allFiles = directory.listAll();
		}
		catch (IOException e) {
			LOG.warn("Skipping {} cleanup for replica shard {}: could not list directory: {}", label, shardNumber, e.getMessage());
			return;
		}

		int deleted = 0;
		for (String fileName : allFiles) {
			if (referenced.contains(fileName)) {
				continue;
			}
			if (IndexWriter.WRITE_LOCK_NAME.equals(fileName)) {
				continue;
			}
			try {
				directory.deleteFile(fileName);
				deleted++;
				LOG.debug("Deleted unreferenced {} file {} from replica shard {}", label, fileName, shardNumber);
			}
			catch (IOException e) {
				LOG.warn("Failed to delete unreferenced {} file {} from replica shard {}: {}", label, fileName, shardNumber, e.getMessage());
			}
		}
		if (deleted > 0) {
			LOG.info("Cleaned up {} unreferenced {} file(s) on replica shard {}", deleted, label, shardNumber);
		}
	}

	public ShardReader createShardReader() throws IOException {
		DirectoryReader indexReader = DirectoryReader.open(indexDirectory);
		DirectoryTaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDirectory);
		taxoReader.setCacheSize(128000);
		return new ShardReader(shardNumber, indexReader, taxoReader, indexConfig, zuliaPerFieldAnalyzer, segmentOpenExecutor, aggregationSettings);
	}

	public void close() throws IOException {
		segmentOpenExecutor.close();
		indexDirectory.close();
		taxoDirectory.close();
	}

}
