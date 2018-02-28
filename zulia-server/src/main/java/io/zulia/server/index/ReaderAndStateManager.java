package io.zulia.server.index;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.search.ReferenceManager;

import java.io.IOException;

public class ReaderAndStateManager extends ReferenceManager<ReaderAndState> {

	public ReaderAndStateManager(IndexWriter writer) throws IOException {
		current = new ReaderAndState(DirectoryReader.open(writer));
	}

	@Override
	protected void decRef(ReaderAndState reference) throws IOException {
		reference.getReader().decRef();
	}

	@Override
	protected ReaderAndState refreshIfNeeded(ReaderAndState referenceToRefresh) throws IOException {

		DirectoryReader reader = DirectoryReader.openIfChanged(referenceToRefresh.getReader());
		if (reader != null) {
			return new ReaderAndState(reader);
		}

		return null;
	}

	@Override
	protected boolean tryIncRef(ReaderAndState reference) {
		return reference.getReader().tryIncRef();
	}

	@Override
	protected int getRefCount(ReaderAndState reference) {
		return reference.getReader().getRefCount();
	}
}
