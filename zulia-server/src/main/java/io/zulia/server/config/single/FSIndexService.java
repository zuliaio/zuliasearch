package io.zulia.server.config.single;

import io.zulia.message.ZuliaIndex;
import io.zulia.server.config.IndexService;

import java.util.List;

public class FSIndexService implements IndexService {
	@Override
	public List<ZuliaIndex.IndexSettings> getIndexes() {
		return null;
	}

	@Override
	public void createIndex(ZuliaIndex.IndexSettings indexSettings) {

	}

	@Override
	public void removeIndex(String indexName) {

	}

	@Override
	public List<ZuliaIndex.IndexMapping> getIndexMappings() {
		return null;
	}

	@Override
	public ZuliaIndex.IndexMapping getIndexMapping(String indexName) {
		return null;
	}

	@Override
	public void storeIndexMapping(ZuliaIndex.IndexMapping indexMapping) {

	}
}
