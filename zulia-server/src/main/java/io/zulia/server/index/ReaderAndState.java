package io.zulia.server.index;

import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.FastTaxonomyFacetCounts;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class ReaderAndState implements AutoCloseable {

	private final FacetsConfig facetsConfig;
	private final DirectoryReader indexReader;
	private final DirectoryTaxonomyReader taxoReader;

	public ReaderAndState(DirectoryReader indexReader, DirectoryTaxonomyReader taxoReader, FacetsConfig facetsConfig) {
		this.indexReader = indexReader;
		this.taxoReader = taxoReader;
		this.facetsConfig = facetsConfig;
	}

	public DirectoryReader getIndexReader() {
		return indexReader;
	}

	@Override
	public void close() throws Exception {
		indexReader.close();
		taxoReader.close();
	}

	public Facets getFacets(FacetsCollector facetsCollector) throws IOException {
		return new FastTaxonomyFacetCounts(taxoReader, facetsConfig, facetsCollector);
	}

	public int getTotalFacets() {
		return taxoReader.getSize();
	}

	public Set<String> getFields() {
		Set<String> fields = new HashSet<>();

		for (LeafReaderContext subReaderContext : indexReader.leaves()) {
			FieldInfos fieldInfos = subReaderContext.reader().getFieldInfos();
			for (FieldInfo fi : fieldInfos) {
				String fieldName = fi.name;
				fields.add(fieldName);
			}
		}
		return fields;

	}

	public int numDocs() {
		return indexReader.numDocs();
	}

	public IndexSearcher getSearcher(PerFieldSimilarityWrapper similarity) {

		IndexSearcher indexSearcher = new IndexSearcher(indexReader);

		//similarity is only set query time, indexing time all these similarities are the same
		indexSearcher.setSimilarity(similarity);

		return indexSearcher;

	}

	public int docFreq(String field, String term) throws IOException {
		return indexReader.docFreq(new Term(field, term));
	}
}
