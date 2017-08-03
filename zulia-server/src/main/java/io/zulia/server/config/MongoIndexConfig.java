package io.zulia.server.config;

import com.mongodb.MongoClient;
import io.zulia.message.ZuliaIndex.IndexMapping;
import io.zulia.message.ZuliaIndex.IndexSettings;

import java.util.List;

public class MongoIndexConfig implements IndexConfig {

	public MongoIndexConfig(MongoClient mongoClient) {

	}

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
