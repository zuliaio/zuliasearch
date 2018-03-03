package io.zulia.server.index;

import org.apache.lucene.facet.sortedset.DefaultSortedSetDocValuesReaderState;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesReaderState;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.index.DirectoryReader;

import java.io.IOException;

public class ReaderAndState implements AutoCloseable {

	private DirectoryReader reader;
	private DirectoryTaxonomyReader taxoReader;

	public ReaderAndState(DirectoryReader reader, DirectoryTaxonomyReader taxoReader) {
		this.reader = reader;
		this.taxoReader = taxoReader;
	}

	public DirectoryReader getReader() {
		return reader;
	}

	public DirectoryTaxonomyReader getTaxoReader() {
		return taxoReader;
	}


	@Override
	public void close() throws Exception {
		reader.close();
		taxoReader.close();
	}

}
