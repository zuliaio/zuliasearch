package io.zulia.server.index;

import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.facet.taxonomy.writercache.TaxonomyWriterCache;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexWriterConfig.OpenMode;
import org.apache.lucene.index.SnapshotDeletionPolicy;
import org.apache.lucene.store.Directory;

import java.io.IOException;

// Ported from Lucene 9's IndexAndTaxonomyRevision.SnapshotDirectoryTaxonomyWriter (removed in Lucene 10).
// Installs a SnapshotDeletionPolicy on the taxonomy's internal IndexWriter so segment replication can grab
// a consistent IndexCommit for the taxo directory.
public class SnapshotDirectoryTaxonomyWriter extends DirectoryTaxonomyWriter {

	private SnapshotDeletionPolicy snapshotDeletionPolicy;

	public SnapshotDirectoryTaxonomyWriter(Directory directory, OpenMode openMode, TaxonomyWriterCache cache) throws IOException {
		super(directory, openMode, cache);
	}

	@Override
	protected IndexWriterConfig createIndexWriterConfig(OpenMode openMode) {
		IndexWriterConfig config = super.createIndexWriterConfig(openMode);
		snapshotDeletionPolicy = new SnapshotDeletionPolicy(config.getIndexDeletionPolicy());
		config.setIndexDeletionPolicy(snapshotDeletionPolicy);
		return config;
	}

	public SnapshotDeletionPolicy getDeletionPolicy() {
		return snapshotDeletionPolicy;
	}
}
