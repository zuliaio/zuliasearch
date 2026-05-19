package io.zulia.server.index.replication;

import io.zulia.server.index.ShardWriteManager;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.store.Directory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;


record CommitSnapshot(ShardWriteManager writeManager, IndexCommit indexCommit, IndexCommit taxoCommit, Directory indexDirectory,
		Collection<String> indexFiles, Directory taxoDirectory, Collection<String> taxoFiles, String indexName, int shardNumber) implements AutoCloseable {

	private final static Logger LOG = LoggerFactory.getLogger(CommitSnapshot.class);

	static CommitSnapshot tryTake(ShardWriteManager writeManager, String indexName, int shardNumber) throws IOException {
		IndexCommit indexCommit;
		try {
			indexCommit = writeManager.snapshotIndex();
		}
		catch (IllegalStateException noCommitYet) {
			LOG.debug("Skipping replication for {}:s{}: {}", indexName, shardNumber, noCommitYet.getMessage());
			return new CommitSnapshot(writeManager, null, null, null, null, null, null, indexName, shardNumber);
		}
		// snapshotIndex pins indexCommit; release on failure or it leaks into SnapshotDeletionPolicy.
		IndexCommit taxoCommit = null;
		try {
			boolean hasTaxonomy = writeManager.hasTaxonomy();
			if (hasTaxonomy) {
				taxoCommit = writeManager.snapshotTaxo();
			}
			Collection<String> indexFiles = indexCommit.getFileNames();
			Collection<String> taxoFiles = hasTaxonomy ? taxoCommit.getFileNames() : null;
			return new CommitSnapshot(writeManager, indexCommit, taxoCommit, writeManager.getIndexDirectory(), indexFiles,
					hasTaxonomy ? writeManager.getTaxoDirectory() : null, taxoFiles, indexName, shardNumber);
		}
		catch (IOException | RuntimeException e) {
			releaseAfterFailure(writeManager, indexCommit, taxoCommit, indexName, shardNumber, e);
			throw e;
		}
	}

	private static void releaseAfterFailure(ShardWriteManager writeManager, IndexCommit indexCommit, IndexCommit taxoCommit, String indexName, int shardNumber,
			Exception primaryException) {
		if (taxoCommit != null) {
			try {
				writeManager.releaseTaxoSnapshot(taxoCommit);
			}
			catch (Exception suppressed) {
				primaryException.addSuppressed(suppressed);
			}
		}
		try {
			writeManager.releaseSnapshot(indexCommit);
		}
		catch (Exception suppressed) {
			primaryException.addSuppressed(suppressed);
		}
		LOG.warn("Released snapshot for {}:s{} after take failure: {}", indexName, shardNumber, primaryException.getMessage());
	}

	boolean isEmpty() {
		return indexCommit == null;
	}

	long generation() {
		return indexCommit.getGeneration();
	}

	@Override
	public void close() {
		if (taxoCommit != null) {
			try {
				writeManager.releaseTaxoSnapshot(taxoCommit);
			}
			catch (Exception e) {
				LOG.warn("Failed to release taxonomy snapshot for {}:s{}", indexName, shardNumber, e);
			}
		}
		if (indexCommit != null) {
			try {
				writeManager.releaseSnapshot(indexCommit);
			}
			catch (Exception e) {
				LOG.warn("Failed to release index snapshot for {}:s{}", indexName, shardNumber, e);
			}
		}
	}
}
