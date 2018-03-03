package io.zulia.server.index;

import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.ReferenceManager;

import java.io.IOException;

public class ShardReaderManager extends ReferenceManager<ShardReader> {

	public ShardReaderManager(ShardWriteManager shardWriteManager) throws IOException {
		DirectoryReader indexReader = DirectoryReader.open(shardWriteManager.getIndexWriter());
		DirectoryTaxonomyReader taxoReader = new DirectoryTaxonomyReader(shardWriteManager.getTaxoWriter());
		current = new ShardReader(indexReader, taxoReader, shardWriteManager.getFacetsConfig());
	}

	@Override
	protected void decRef(ShardReader reference) throws IOException {
		reference.decRef();
	}

	@Override
	protected ShardReader refreshIfNeeded(ShardReader referenceToRefresh) throws IOException {
		return referenceToRefresh.refreshIfNeeded();
	}

	@Override
	protected boolean tryIncRef(ShardReader reference) throws IOException {
		return reference.tryIncRef();
	}

	@Override
	protected int getRefCount(ShardReader reference) {
		return reference.getRefCount();
	}
}
