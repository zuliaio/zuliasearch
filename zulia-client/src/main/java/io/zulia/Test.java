package io.zulia;

import io.zulia.client.command.CreateIndex;
import io.zulia.client.command.Query;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.config.ZuliaPoolConfig;
import io.zulia.client.pool.ZuliaWorkPool;

public class Test {
	public static void main(String[] args) throws Exception {
		ZuliaPoolConfig zuliaPoolConfig = new ZuliaPoolConfig().addNode("localhost", 32191).addNode("localhost", 32193);
		ZuliaWorkPool zuliaPool = new ZuliaWorkPool(zuliaPoolConfig);
		zuliaPool.updateNodes();

		Query query = new Query("indexC", null, 10);
		zuliaPool.query(query);

		//createIndex(zuliaPool, "indexA");
		//createIndex(zuliaPool, "indexB");
		//createIndex(zuliaPool, "indexC");
	}

	private static void createIndex(ZuliaWorkPool zuliaPool, String indexName) throws Exception {
		ClientIndexConfig clientIndexConfig = new ClientIndexConfig();
		clientIndexConfig.setIndexName(indexName);
		CreateIndex createIndex = new CreateIndex(clientIndexConfig);
		zuliaPool.createIndex(createIndex);
	}
}
