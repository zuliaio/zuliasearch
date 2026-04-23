package io.zulia.client.command;

import io.zulia.client.command.base.SimpleCommand;
import io.zulia.client.pool.ZuliaConnection;
import io.zulia.client.result.CreateIndexAliasResult;
import io.zulia.message.ZuliaIndex.IndexAlias;
import io.zulia.message.ZuliaServiceOuterClass.CreateIndexAliasRequest;
import io.zulia.message.ZuliaServiceOuterClass.CreateIndexAliasResponse;

import java.util.List;

import static io.zulia.message.ZuliaServiceGrpc.ZuliaServiceBlockingStub;

/**
 * Creates or updates am index alias
 *
 * @author mdavis
 */
public class CreateIndexAlias extends SimpleCommand<CreateIndexAliasRequest, CreateIndexAliasResult> {

	private final IndexAlias indexAlias;

	public CreateIndexAlias(IndexAlias indexAlias) {
		this.indexAlias = indexAlias;
	}

	public CreateIndexAlias(String aliasName, String indexName) {
		this.indexAlias = IndexAlias.newBuilder().setAliasName(aliasName).addIndexNames(indexName).build();
	}

	public CreateIndexAlias(String aliasName, List<String> indexNames) {
		this(aliasName, indexNames, null);
	}

	public CreateIndexAlias(String aliasName, List<String> indexNames, String writeIndex) {
		IndexAlias.Builder builder = IndexAlias.newBuilder().setAliasName(aliasName).addAllIndexNames(indexNames);
		if (writeIndex != null && !writeIndex.isEmpty()) {
			builder.setWriteIndex(writeIndex);
		}
		this.indexAlias = builder.build();
	}

	@Override
	public CreateIndexAliasRequest getRequest() {
		return CreateIndexAliasRequest.newBuilder().setIndexAlias(indexAlias).build();
	}

	@Override
	public CreateIndexAliasResult execute(ZuliaConnection zuliaConnection) {
		ZuliaServiceBlockingStub service = zuliaConnection.getService();
		CreateIndexAliasResponse createIndexResponse = service.createIndexAlias(getRequest());
		return new CreateIndexAliasResult(createIndexResponse);
	}

}
