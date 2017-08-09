package io.zulia.client.command;

import io.zulia.client.command.base.SimpleCommand;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaConnection;
import io.zulia.message.ZuliaServiceOuterClass.GetIndexSettingsRequest;

import static io.zulia.message.ZuliaServiceGrpc.ZuliaServiceBlockingStub;
import static io.zulia.message.ZuliaServiceOuterClass.GetIndexSettingsResponse;

/**
 * Created by Payam Meyer on 4/3/17.
 * @author pmeyer
 */
public class GetIndexConfig extends SimpleCommand<GetIndexSettingsRequest, GetIndexConfigResult> {

	private final String indexName;

	public GetIndexConfig(String indexName) {
		this.indexName = indexName;
	}

	@Override
	public GetIndexSettingsRequest getRequest() {
		return GetIndexSettingsRequest.newBuilder().setIndexName(indexName).build();
	}

	@Override
	public GetIndexConfigResult execute(ZuliaConnection zuliaConnection) {

		ZuliaServiceBlockingStub service = zuliaConnection.getService();

		GetIndexSettingsResponse getIndexConfigResponse = service.getIndexSettings(getRequest());

		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.configure(getIndexConfigResponse.getIndexSettings());

		return new GetIndexConfigResult(indexConfig);
	}
}
