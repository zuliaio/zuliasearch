package io.zulia.client.command;

import io.zulia.client.ZuliaRESTClient;
import io.zulia.client.command.base.RESTCommand;
import io.zulia.client.command.base.ShardRoutableCommand;
import io.zulia.client.result.StoreLargeAssociatedResult;
import org.bson.Document;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;

public class StoreLargeAssociated extends RESTCommand<StoreLargeAssociatedResult> implements ShardRoutableCommand {

	private String uniqueId;
	private String fileName;
	private String indexName;
	private File fileToStore;
	private InputStream source;
	private Document meta;

	public StoreLargeAssociated(String uniqueId, String indexName, String fileName, File fileToStore) {
		this.uniqueId = uniqueId;
		this.fileName = fileName;
		this.indexName = indexName;
		this.fileToStore = fileToStore;
	}

	public StoreLargeAssociated(String uniqueId, String indexName, String fileName, InputStream source) {
		this.uniqueId = uniqueId;
		this.fileName = fileName;
		this.indexName = indexName;
		this.source = source;
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
		InputStream input = source;
		if (fileToStore != null) {
			input = new FileInputStream(fileToStore);
		}

		if (input != null) {
			zuliaRESTClient.storeAssociated(uniqueId, indexName, fileName, meta, input);
		}
		else {
			throw new Exception("File or input stream must be set");
		}
		return new StoreLargeAssociatedResult();
	}

}
