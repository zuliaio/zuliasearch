package io.zulia.server.config;

import io.zulia.message.ZuliaIndex.IndexMapping;
import io.zulia.message.ZuliaIndex.IndexSettings;

import java.util.List;

public class MongoIndexConfig implements IndexConfig {

	@Override
	public List<IndexSettings> getIndexes() {
		return null;
	}

	@Override
	public void createIndex(IndexSettings indexSettings) {

	}

	@Override
	public void removeIndex(String indexName) {

	}

	@Override
	public List<IndexMapping> getIndexMappings() {
		return null;
	}

	@Override
	public IndexMapping getIndexMapping(String indexName) {
		return null;
	}

	@Override
	public void storeIndexMapping(IndexMapping indexMapping) {

	}
}
