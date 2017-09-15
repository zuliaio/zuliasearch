package io.zulia.server.config;

import io.zulia.message.ZuliaIndex;
import io.zulia.message.ZuliaIndex.IndexMapping;
import io.zulia.message.ZuliaIndex.IndexSettings;

import java.util.List;

public interface IndexService {

	String SETTINGS = "settings";

	/**
	 *
	 * @return - all indexes for the cluster, {@code null} if nothing exists
	 */
	List<IndexSettings> getIndexes() throws Exception;

	/**
	 *
	 * @param indexName - the index name to return
	 * @return the index settings for an index
	 * @throws Exception
	 */
	IndexSettings getIndex(String indexName) throws Exception;

	/**
	 * create or update an index
	 * shards cannot be changed after an index is created
	 * time fields will be handled by the config storage
	 *
	 * @param indexSettings
	 */
	void createIndex(ZuliaIndex.IndexSettings indexSettings) throws Exception;

	/**
	 * remove an index from the config storage
	 * @param indexName
	 */
	void removeIndex(String indexName) throws Exception;

	/**
	 *
	 * @return - returns all index mapping for a cluster, {@code null} if nothing exists
	 */
	List<IndexMapping> getIndexMappings() throws Exception;

	/**
	 *
	 * @param indexName - index name to fetch server mapping from
	 * @return - index mapping for an index, {@code null} if nothing exists
	 */
	IndexMapping getIndexMapping(String indexName) throws Exception;

	/**
	 * Creates or updates index mapping
	 * @param indexMapping - index mapping to create or update
	 */
	void storeIndexMapping(IndexMapping indexMapping) throws Exception;

	/**
	 * remove an index mapping from the config storage
	 * @param indexName - index name of mapping to remove
	 */
	void removeIndexMapping(String indexName) throws Exception;

}
