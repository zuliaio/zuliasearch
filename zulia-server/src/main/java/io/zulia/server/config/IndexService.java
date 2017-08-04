package io.zulia.server.config;

import io.zulia.message.ZuliaIndex.IndexMapping;
import io.zulia.message.ZuliaIndex.IndexSettings;

import java.util.List;

public interface IndexService {

	/**
	 *
	 * @return - all indexes for the cluster, {@code null} if nothing exists
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

	/**
	 *
	 * @return - returns all index mapping for a cluster, {@code null} if nothing exists
	 */
	List<IndexMapping> getIndexMappings();

	/**
	 *
	 * @param indexName -index name to fetch server mapping from
	 * @return - index mapping for an index, {@code null} if nothing exists
	 */
	IndexMapping getIndexMapping(String indexName);

	/**
	 * Creates or updates index mapping
	 * @param indexMapping - index mapping to create or update
	 */
	void storeIndexMapping(IndexMapping indexMapping);

}
