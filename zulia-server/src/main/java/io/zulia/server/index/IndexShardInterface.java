package io.zulia.server.index;

import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.IndexWriter;

/**
 * Created by Matt Davis on 7/29/15.
 * @author mdavis
 */
public interface IndexShardInterface {
	IndexWriter getIndexWriter(int shardNumber) throws Exception;

	PerFieldAnalyzerWrapper getPerFieldAnalyzer() throws Exception;

	DirectoryTaxonomyWriter getTaxoWriter(int shardNumber) throws Exception;
}
