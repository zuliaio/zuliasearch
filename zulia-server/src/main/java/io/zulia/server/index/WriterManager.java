package io.zulia.server.index;

import io.zulia.server.analysis.ZuliaPerFieldAnalyzer;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NRTCachingDirectory;

import java.io.IOException;
import java.nio.file.Path;

public class WriterManager {

	private final ZuliaPerFieldAnalyzer zuliaPerFieldAnalyzer;
	private IndexWriter indexWriter;
	private DirectoryTaxonomyWriter taxoWriter;

	public WriterManager(Path pathToIndex, Path pathToTaxoIndex, ZuliaPerFieldAnalyzer zuliaPerFieldAnalyzer) throws IOException {

		this.zuliaPerFieldAnalyzer = zuliaPerFieldAnalyzer;

		openIndexWriter(pathToIndex);
		openTaxoWriter(pathToTaxoIndex);

	}

	private void openIndexWriter(Path pathToIndex) throws IOException {

		Directory d = MMapDirectory.open(pathToIndex);

		IndexWriterConfig config = new IndexWriterConfig(zuliaPerFieldAnalyzer);
		config.setIndexDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());
		config.setMaxBufferedDocs(Integer.MAX_VALUE);
		config.setRAMBufferSizeMB(128); // should be overwritten by ZuliaShard.updateIndexSettings()
		config.setUseCompoundFile(false);

		NRTCachingDirectory nrtCachingDirectory = new NRTCachingDirectory(d, 32, 128);

		this.indexWriter = new	IndexWriter(nrtCachingDirectory, config);

	}

	private void openTaxoWriter(Path pathToTaxo) throws IOException {
		Directory d = MMapDirectory.open(pathToTaxo);
		NRTCachingDirectory nrtCachingDirectory = new NRTCachingDirectory(d, 8, 16);
		this.taxoWriter = new DirectoryTaxonomyWriter(nrtCachingDirectory);

	}

	public void close() {
		if (indexWriter != null) {
			Directory directory = indexWriter.getDirectory();
			try {
				indexWriter.close();
			}
			catch (Exception e) {

			}
			finally {
				indexWriter = null;
				try {
					directory.close();
				}
				catch (Exception e) {

				}

			}
		}
		if (taxoWriter != null) {
			Directory directory = taxoWriter.getDirectory();
			try {
				taxoWriter.close();
			}
			catch (Exception e) {

			}
			finally {
				taxoWriter = null;
				try {
					directory.close();
				}
				catch (Exception e) {

				}

			}
		}
	}

	public Analyzer getAnalyzer() {
		return zuliaPerFieldAnalyzer;
	}

	public IndexWriter getIndexWriter() {
		return indexWriter;
	}

	public DirectoryTaxonomyWriter getTaxoWriter() {
		return taxoWriter;
	}

	public void commit() throws IOException {
		indexWriter.commit();
		taxoWriter.commit();
	}


}
