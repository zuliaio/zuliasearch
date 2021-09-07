package io.zulia.client.command;

import io.zulia.client.command.base.RESTCommand;
import io.zulia.client.command.base.ShardRoutableCommand;
import io.zulia.client.rest.ZuliaRESTClient;
import io.zulia.client.result.StoreLargeAssociatedResult;
import org.bson.Document;

public class StoreLargeAssociated extends RESTCommand<StoreLargeAssociatedResult> implements ShardRoutableCommand {

	private String uniqueId;
	private String fileName;
	private String indexName;
	private Document meta;
	private byte[] bytes;

	public StoreLargeAssociated(String uniqueId, String indexName, String fileName, byte[] bytes) {
		this.uniqueId = uniqueId;
		this.fileName = fileName;
		this.indexName = indexName;
		this.bytes = bytes;
	}

	public Document getMeta() {
		return meta;
	}

	public StoreLargeAssociated setMeta(Document meta) {
		this.meta = meta;
		return this;
	}

	@Override
	public String getUniqueId() {
		return uniqueId;
	}

	@Override
	public String getIndexName() {
		return indexName;
	}

	@Override
	public StoreLargeAssociatedResult execute(ZuliaRESTClient zuliaRESTClient) throws Exception {
		if (bytes != null) {
			zuliaRESTClient.storeAssociated(uniqueId, indexName, fileName, meta, bytes);
		}
		else {
			throw new Exception("File or input stream must be set");
		}
		return new StoreLargeAssociatedResult();
	}

}
