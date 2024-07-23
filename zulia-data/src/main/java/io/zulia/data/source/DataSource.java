package io.zulia.data.source;

public interface DataSource<T extends DataSourceRecord> extends Iterable<T>, AutoCloseable {

	/**
	 * resets the iterator to the beginning of the source
	 *
	 * @throws Exception
	 */
	void reset() throws Exception;

	@Override
	void close() throws Exception;
}
