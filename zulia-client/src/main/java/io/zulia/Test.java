package io.zulia;

import io.zulia.client.command.CreateIndex;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.config.ZuliaPoolConfig;
import io.zulia.client.pool.ZuliaWorkPool;

public class Test {
	public static void main(String[] args) throws Exception {
		ZuliaPoolConfig zuliaPoolConfig = new ZuliaPoolConfig().addNode("localhost");
		ZuliaWorkPool zuliaPool = new ZuliaWorkPool(zuliaPoolConfig);

		ClientIndexConfig clientIndexConfig = new ClientIndexConfig();
		clientIndexConfig.setIndexName("test");
		//clientIndexConfig.setStoreDocumentInIndex();
		CreateIndex createIndex = new CreateIndex(clientIndexConfig);

		zuliaPool.createIndex(createIndex);
	}
}
