package io.zulia.server.config;

import io.zulia.message.ZuliaIndex;
import io.zulia.message.ZuliaIndex.IndexAlias;
import io.zulia.message.ZuliaIndex.IndexSettings;
import io.zulia.message.ZuliaIndex.IndexShardMapping;

import java.util.List;

public interface IndexService {

	/**
	 * @return - all indexes for the cluster
	 */
	List<IndexSettings> getIndexes() throws Exception;

	/**
	 * @param indexName - the index name to return
	 * @return the index settings for an index
	 * @throws Exception
	 */
	IndexSettings getIndex(String indexName) throws Exception;

	/**
	 * create or update an index number of shards cannot be changed after an index is created time fields will be handled by the config storage
	 *
	 * @param indexSettings
	 */
	void storeIndex(ZuliaIndex.IndexSettings indexSettings) throws Exception;

	/**
	 * remove an index from the config storage
	 *
	 * @param indexName
	 */
	void removeIndex(String indexName) throws Exception;

	/**
	 * @return - returns all index mapping for a cluster
	 */
	List<IndexShardMapping> getIndexMappings() throws Exception;

	/**
	 * @param indexName - index name to fetch server mapping from
	 * @return - index mapping for an index
	 */
	IndexShardMapping getIndexMapping(String indexName) throws Exception;

	/**
	 * Creates or updates index mapping
	 *
	 * @param indexMapping - index mapping to create or update
	 */
	void storeIndexMapping(IndexShardMapping indexMapping) throws Exception;

	/**
	 * remove an index mapping from the config storage
	 *
	 * @param indexName - index name of mapping to remove
	 */
	void removeIndexMapping(String indexName) throws Exception;

	/**
	 * @return - returns all index aliases for a cluster
	 */
	List<IndexAlias> getIndexAliases() throws Exception;

	/**
	 * @param indexAlias - index alias to fetch
	 * @return - index alias
	 */
	IndexAlias getIndexAlias(String indexAlias) throws Exception;

	/**
	 * Creates or updates index alias
	 *
	 * @param indexAlias - index alias to create or update
	 */
	void storeIndexAlias(IndexAlias indexAlias) throws Exception;

	/**
	 * remove an index alias from the config storage
	 *
	 * @param indexAlias - index alias to remove
	 */
	void removeIndexAlias(String indexAlias) throws Exception;

}
