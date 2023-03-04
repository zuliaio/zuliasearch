package io.zulia.client.command;

import io.zulia.client.command.base.SimpleCommand;
import io.zulia.client.command.base.SingleIndexRoutableCommand;
import io.zulia.client.pool.ZuliaConnection;
import io.zulia.client.result.DeleteIndexAliasResult;
import io.zulia.message.ZuliaServiceOuterClass.DeleteIndexAliasRequest;
import io.zulia.message.ZuliaServiceOuterClass.DeleteIndexAliasResponse;

import static io.zulia.message.ZuliaServiceGrpc.ZuliaServiceBlockingStub;

/**
 * Deletes an index alias.  If index alias does not exist throws an exception
 *
 * @author mdavis
 */
public class DeleteIndexAlias extends SimpleCommand<DeleteIndexAliasRequest, DeleteIndexAliasResult> implements SingleIndexRoutableCommand {

	private String aliasName;

	public DeleteIndexAlias(String aliasName) {
		this.aliasName = aliasName;
	}

	@Override
	public String getIndexName() {
		return aliasName;
	}

	@Override
	public DeleteIndexAliasRequest getRequest() {
		return DeleteIndexAliasRequest.newBuilder().setAliasName(aliasName).build();
	}

	@Override
	public DeleteIndexAliasResult execute(ZuliaConnection zuliaConnection) {
		ZuliaServiceBlockingStub service = zuliaConnection.getService();

		DeleteIndexAliasResponse indexAliasDeleteResponse = service.deleteIndexAlias(getRequest());

		return new DeleteIndexAliasResult(indexAliasDeleteResponse);
	}

}
