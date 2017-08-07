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
		this.fileName = fileName;
		this.outputFile = outputFile;
		this.indexName = indexName;
	}

	public FetchLargeAssociated(String uniqueId, String indexName, String fileName, OutputStream destination) {
		this.uniqueId = uniqueId;
		this.fileName = fileName;
		this.destination = destination;
		this.indexName = indexName;
	}

	@Override
	public FetchLargeAssociatedResult execute(ZuliaRESTClient zuliaRESTClient) throws Exception {
		if (outputFile != null) {
			zuliaRESTClient.fetchAssociated(uniqueId, indexName, fileName, outputFile);
		}
		else if (destination != null) {
			zuliaRESTClient.fetchAssociated(uniqueId, indexName, fileName, destination);
		}
		else {
			throw new Exception("File or output stream must be set");
		}
		return new FetchLargeAssociatedResult();
	}

}
