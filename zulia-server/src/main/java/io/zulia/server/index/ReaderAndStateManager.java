package io.zulia.server.index;

import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.ReferenceManager;

import java.io.IOException;

public class ReaderAndStateManager extends ReferenceManager<ReaderAndState> {



	public ReaderAndStateManager(WriterManager writerManager) throws IOException {
		current = new ReaderAndState(DirectoryReader.open(writerManager.getIndexWriter()), new DirectoryTaxonomyReader(writerManager.getTaxoWriter()));
	}

	@Override
	protected void decRef(ReaderAndState reference) throws IOException {
		reference.getIndexReader().decRef();
	}

	@Override
	protected ReaderAndState refreshIfNeeded(ReaderAndState referenceToRefresh) throws IOException {

		DirectoryReader reader = DirectoryReader.openIfChanged(referenceToRefresh.getIndexReader());
		if (reader != null) {
			return new ReaderAndState(reader);
		}

		return null;
	}

	@Override
	protected boolean tryIncRef(ReaderAndState reference) throws IOException {
		if (reference.getIndexReader().tryIncRef()) {
			if (reference.getTaxoReader().tryIncRef()) {
				return true;
			} else {
				reference.getIndexReader().decRef();
			}
		}
		return false;
	}

	@Override
	protected int getRefCount(ReaderAndState reference) {
		return reference.getIndexReader().getRefCount();
	}
}
