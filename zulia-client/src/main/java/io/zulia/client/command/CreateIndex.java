package io.zulia.client.command;

import io.zulia.client.command.base.SimpleCommand;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaConnection;
import io.zulia.client.result.CreateIndexResult;
import io.zulia.message.ZuliaServiceOuterClass.CreateIndexRequest;

import static io.zulia.message.ZuliaServiceGrpc.ZuliaServiceBlockingStub;
import static io.zulia.message.ZuliaServiceOuterClass.CreateIndexResponse;

/**
 * Creates a new index with the name, number of segments, unique id field, and IndexSettings given.  Whether the index supports faceting
 * or not is also configurable.  However, only the IndexConfig cannot be changed after the index is created.  If index already exists an exception will be thrown.
 * See @CreateOrUpdateIndex
 *
 * @author mdavis
 *
 */
public class CreateIndex extends SimpleCommand<CreateIndexRequest, CreateIndexResult> {

	private ClientIndexConfig indexConfig;

	public CreateIndex(ClientIndexConfig indexConfig) {
		this.indexConfig = indexConfig;
	}

	@Override
	public CreateIndexRequest getRequest() {
		CreateIndexRequest.Builder createIndexRequestBuilder = CreateIndexRequest.newBuilder();

		if (indexConfig != null) {
			createIndexRequestBuilder.setIndexSettings(indexConfig.getIndexSettings());
		}

		return createIndexRequestBuilder.build();
	}

	@Override
	public CreateIndexResult execute(ZuliaConnection zuliaConnection) {
		ZuliaServiceBlockingStub service = zuliaConnection.getService();
		CreateIndexResponse createIndexResponse = service.createIndex(getRequest());

		return new CreateIndexResult(createIndexResponse);
	}

}
