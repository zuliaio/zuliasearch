package io.zulia.server;

import io.zulia.message.ZuliaIndex.IndexSettings;

import java.util.List;

public interface IndexConfig {

	/**
	 *
	 * @return - all indexes for the cluster
	 */
	List<IndexSettings> getIndexes();

	/**
	 * create or update an index
	 * shards cannot be changed after an index is created
	 * time fields will be handled by the config storage
	 *
	 * @param indexSettings
	 */
	void createIndex(IndexSettings indexSettings);

	/**
	 * remove an index from the config storage
	 * @param indexName
	 */
	void removeIndex(String indexName);

}
