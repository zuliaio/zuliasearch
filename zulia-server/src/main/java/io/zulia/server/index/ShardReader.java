package io.zulia.server.index;

import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.FastTaxonomyFacetCounts;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
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

public class ShardReader implements AutoCloseable {

	private final FacetsConfig facetsConfig;
	private final DirectoryReader indexReader;
	private final DirectoryTaxonomyReader taxoReader;

	public ShardReader(DirectoryReader indexReader, DirectoryTaxonomyReader taxoReader, FacetsConfig facetsConfig) {
		this.indexReader = indexReader;
		this.taxoReader = taxoReader;
		this.facetsConfig = facetsConfig;
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

	public ShardTermsHandler getShardTermsHandler() {
		return new ShardTermsHandler(indexReader);
	}

	public int getRefCount() {
		return indexReader.getRefCount();
	}

	public boolean tryIncRef() throws IOException {
		if (indexReader.tryIncRef()) {
			if (taxoReader.tryIncRef()) {
				return true;
			}
			else {
				indexReader.decRef();
			}
		}
		return false;
	}

	public void decRef() throws IOException {
		indexReader.decRef();
		taxoReader.decRef();
	}

	public ShardReader refreshIfNeeded() throws IOException {

		DirectoryReader r = DirectoryReader.openIfChanged(indexReader);
		if (r == null) {
			return null;
		}
		else {
			DirectoryTaxonomyReader tr = TaxonomyReader.openIfChanged(taxoReader);
			if (tr == null) {
				taxoReader.incRef();
				tr = taxoReader;
			}

			return new ShardReader(r, tr, facetsConfig);
		}

	}

}
