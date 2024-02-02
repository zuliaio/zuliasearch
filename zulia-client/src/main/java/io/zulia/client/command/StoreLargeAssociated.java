package io.zulia.client.command;

import io.zulia.client.command.base.RESTCommand;
import io.zulia.client.command.base.ShardRoutableCommand;
import io.zulia.client.rest.ZuliaRESTClient;
import io.zulia.client.result.StoreLargeAssociatedResult;
import org.bson.Document;

import java.io.File;
import java.io.InputStream;

public class StoreLargeAssociated extends RESTCommand<StoreLargeAssociatedResult> implements ShardRoutableCommand {

	private final String uniqueId;
	private final String fileName;
	private final String indexName;
	private Document meta;
	private byte[] bytes;
	private File file;
	private InputStream inputStream;
	private boolean closeStream;

	public StoreLargeAssociated(String uniqueId, String indexName, String fileName, byte[] bytes) {
		this.uniqueId = uniqueId;
		this.fileName = fileName;
		this.indexName = indexName;
		this.bytes = bytes;
	}

	public StoreLargeAssociated(String uniqueId, String indexName, String fileName, InputStream inputStream, boolean closeStream) {
		this.uniqueId = uniqueId;
		this.fileName = fileName;
		this.indexName = indexName;
		this.inputStream = inputStream;
		this.closeStream = closeStream;
	}

	public StoreLargeAssociated(String uniqueId, String indexName, String fileName, File file) {
		this.uniqueId = uniqueId;
		this.fileName = fileName;
		this.indexName = indexName;
		this.file = file;
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
			zuliaRESTClient.storeAssociated(indexName, uniqueId, fileName, meta, bytes);
		}
		else if (file != null) {
			zuliaRESTClient.storeAssociated(indexName, uniqueId, fileName, meta, file);
		}
		else if (inputStream != null) {
			zuliaRESTClient.storeAssociated(indexName, uniqueId, fileName, meta, inputStream, closeStream);
		}
		else {
			throw new Exception("File, byte[], or InputStream must be set");
		}
		return new StoreLargeAssociatedResult();
	}

}
