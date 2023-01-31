package io.zulia.client.command;

import io.zulia.client.command.base.SimpleCommand;
import io.zulia.client.pool.ZuliaConnection;
import io.zulia.client.result.CreateIndexAliasResult;
import io.zulia.message.ZuliaIndex.IndexAlias;
import io.zulia.message.ZuliaServiceOuterClass.CreateIndexAliasRequest;
import io.zulia.message.ZuliaServiceOuterClass.CreateIndexAliasResponse;

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
        this.indexAlias = IndexAlias.newBuilder().setAliasName(aliasName).setIndexName(indexName).build();
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
