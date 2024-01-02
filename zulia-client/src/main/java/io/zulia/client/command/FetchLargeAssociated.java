package io.zulia.client.command;

import io.zulia.client.command.base.RESTCommand;
import io.zulia.client.command.base.ShardRoutableCommand;
import io.zulia.client.rest.ZuliaRESTClient;
import io.zulia.client.result.FetchLargeAssociatedResult;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;

public class FetchLargeAssociated extends RESTCommand<FetchLargeAssociatedResult> implements ShardRoutableCommand {
	private final String uniqueId;

	private final String indexName;

	private String fileName;
	private File outputFile;
	private OutputStream destination;

	private boolean closeStream;

	public FetchLargeAssociated(String uniqueId, String indexName, String fileName, File outputFile) {
		this.uniqueId = uniqueId;
		this.indexName = indexName;
		this.fileName = fileName;
		this.outputFile = outputFile;
	}

	/**
	 * @param uniqueId
	 * @param indexName
	 * @param fileName
	 * @param destination - OutputStream for a file with the given filename for the unique id
	 */
	public FetchLargeAssociated(String uniqueId, String indexName, String fileName, OutputStream destination, boolean closeStream) {
		this.uniqueId = uniqueId;
		this.indexName = indexName;
		this.fileName = fileName;
		this.destination = destination;
		this.closeStream = closeStream;
	}

	/**
	 * @param uniqueId
	 * @param indexName
	 * @param destination - OutputStream for a zip file of all files for the unique id
	 */
	public FetchLargeAssociated(String uniqueId, String indexName, OutputStream destination, boolean closeStream) {
		this.uniqueId = uniqueId;
		this.indexName = indexName;
		this.destination = destination;
		this.closeStream = closeStream;
	}

	public FetchLargeAssociated(String uniqueId, String indexName, File outputFile) {
		this.uniqueId = uniqueId;
		this.indexName = indexName;
		this.outputFile = outputFile;
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
	public FetchLargeAssociatedResult execute(ZuliaRESTClient zuliaRESTClient) throws Exception {
		if (outputFile != null) {
			if (fileName != null) {
				zuliaRESTClient.fetchAssociated(indexName, uniqueId, fileName, new FileOutputStream(outputFile), true);
			}
			else {
				zuliaRESTClient.fetchAssociatedBundle(indexName, uniqueId, new FileOutputStream(outputFile), true);
			}
		}
		else if (destination != null) {
			if (fileName != null) {
				zuliaRESTClient.fetchAssociated(indexName, uniqueId, fileName, destination, closeStream);
			}
			else {
				zuliaRESTClient.fetchAssociatedBundle(indexName, uniqueId, destination, closeStream);
			}
		}
		else {
			throw new Exception("A writer must be set");
		}
		return new FetchLargeAssociatedResult();
	}

}
