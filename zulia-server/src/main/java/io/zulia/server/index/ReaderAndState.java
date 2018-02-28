package io.zulia.server.index;

import org.apache.lucene.facet.sortedset.DefaultSortedSetDocValuesReaderState;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesReaderState;
import org.apache.lucene.index.DirectoryReader;

import java.io.IOException;

public class ReaderAndState implements AutoCloseable {

	private DirectoryReader reader;
	private SortedSetDocValuesReaderState sortedSetDocValuesReaderState;

	public ReaderAndState(DirectoryReader reader) throws IOException {
		this.reader = reader;
		try {
			this.sortedSetDocValuesReaderState = new DefaultSortedSetDocValuesReaderState(reader);
		}
		catch (IllegalArgumentException e) {

		}
	}

	public DirectoryReader getReader() {
		return reader;
	}

	public SortedSetDocValuesReaderState getSortedSetDocValuesReaderState() {
		return sortedSetDocValuesReaderState;
	}

	@Override
	public void close() throws Exception {
		reader.close();
	}

}
