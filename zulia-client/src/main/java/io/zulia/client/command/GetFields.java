package io.zulia.client.command;

import io.zulia.client.command.base.SimpleCommand;
import io.zulia.client.command.base.SingleIndexRoutableCommand;
import io.zulia.client.pool.ZuliaConnection;
import io.zulia.client.result.GetFieldsResult;
import io.zulia.message.ZuliaServiceOuterClass;

import static io.zulia.message.ZuliaServiceGrpc.ZuliaServiceBlockingStub;

/**
 * Returns all the fields from a given index
 * @author mdavis
 *
 */
public class GetFields extends SimpleCommand<ZuliaServiceOuterClass.GetFieldNamesRequest, GetFieldsResult> implements SingleIndexRoutableCommand {

	private String indexName;

	public GetFields(String indexName) {
		this.indexName = indexName;
	}

	@Override
	public String getIndexName() {
		return indexName;
	}

	@Override
	public ZuliaServiceOuterClass.GetFieldNamesRequest getRequest() {
		return ZuliaServiceOuterClass.GetFieldNamesRequest.newBuilder().setIndexName(indexName).build();
	}

	@Override
	public GetFieldsResult execute(ZuliaConnection zuliaConnection) {
		ZuliaServiceBlockingStub service = zuliaConnection.getService();

		ZuliaServiceOuterClass.GetFieldNamesResponse getFieldNamesResponse = service.getFieldNames(getRequest());

		return new GetFieldsResult(getFieldNamesResponse);
	}

}
