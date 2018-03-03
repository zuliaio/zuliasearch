package io.zulia.server.index;

import io.zulia.server.analysis.ZuliaPerFieldAnalyzer;
import io.zulia.server.config.ServerIndexConfig;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NRTCachingDirectory;

import java.io.IOException;
import java.nio.file.Path;

public class WriterManager {

	private final ZuliaPerFieldAnalyzer zuliaPerFieldAnalyzer;
	private final FacetsConfig facetsConfig;

	private IndexWriter indexWriter;
	private DirectoryTaxonomyWriter taxoWriter;

	public WriterManager(Path pathToIndex, Path pathToTaxoIndex, FacetsConfig facetsConfig, ZuliaPerFieldAnalyzer zuliaPerFieldAnalyzer) throws IOException {

		this.zuliaPerFieldAnalyzer = zuliaPerFieldAnalyzer;
		this.facetsConfig = facetsConfig;

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

		this.indexWriter = new IndexWriter(nrtCachingDirectory, config);

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

	public void updateIndexSetting(ServerIndexConfig indexConfig) {
		int ramBufferMB = indexConfig.getRAMBufferMB() != 0 ? indexConfig.getRAMBufferMB() : 128;
		indexWriter.getConfig().setRAMBufferSizeMB(ramBufferMB);
	}

	public void deleteDocuments(Term term) throws IOException {
		indexWriter.deleteDocuments(term);
	}

	public void forceMerge(int maxNumberSegments) throws IOException {
		indexWriter.forceMerge(maxNumberSegments);
	}

	public void updateDocument(Document luceneDocument, Term updateQuery) throws IOException {
		luceneDocument = facetsConfig.build(taxoWriter, luceneDocument);
		indexWriter.updateDocument(updateQuery, luceneDocument);
	}

	public void deleteAll() throws IOException {
		indexWriter.deleteAll();
	}
}
