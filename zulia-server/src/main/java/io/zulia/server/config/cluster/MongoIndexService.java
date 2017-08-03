package io.zulia.server.config.cluster;

import com.mongodb.MongoClient;
import io.zulia.message.ZuliaIndex.IndexMapping;
import io.zulia.message.ZuliaIndex.IndexSettings;
import io.zulia.server.config.IndexService;

import java.util.List;

public class MongoIndexService implements IndexService {

	public MongoIndexService(MongoClient mongoClient) {

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
