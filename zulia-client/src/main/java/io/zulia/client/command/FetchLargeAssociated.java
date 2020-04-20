package io.zulia.client.command;

import io.zulia.client.ZuliaRESTClient;
import io.zulia.client.command.base.RESTCommand;
import io.zulia.client.result.FetchLargeAssociatedResult;

import java.io.File;
import java.io.OutputStream;

public class FetchLargeAssociated extends RESTCommand<FetchLargeAssociatedResult> {

	private String uniqueId;
	private String fileName;
	private File outputFile;
	private OutputStream destination;
	private String indexName;

	public FetchLargeAssociated(String uniqueId, String indexName, String fileName, File outputFile) {
		this.uniqueId = uniqueId;
		this.indexName = indexName;
		this.fileName = fileName;
		this.outputFile = outputFile;
	}

	public FetchLargeAssociated(String uniqueId, String indexName, String fileName, OutputStream destination) {
		this.uniqueId = uniqueId;
		this.indexName = indexName;
		this.fileName = fileName;
		this.destination = destination;
	}

	public FetchLargeAssociated(String uniqueId, String indexName, OutputStream destination) {
		this.uniqueId = uniqueId;
		this.indexName = indexName;
		this.destination = destination;
	}

	@Override
	public FetchLargeAssociatedResult execute(ZuliaRESTClient zuliaRESTClient) throws Exception {
		if (outputFile != null) {
			zuliaRESTClient.fetchAssociated(uniqueId, indexName, fileName, outputFile);
		}
		else if (destination != null) {
			if (fileName != null) {
				zuliaRESTClient.fetchAssociated(uniqueId, indexName, fileName, destination);
			}
			else {
				zuliaRESTClient.fetchAssociated(uniqueId, indexName, destination);
			}
		}
		else {
			throw new Exception("A writer must be set");
		}
		return new FetchLargeAssociatedResult();
	}

}
