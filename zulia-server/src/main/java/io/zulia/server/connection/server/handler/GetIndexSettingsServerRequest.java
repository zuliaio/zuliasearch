package io.zulia.server.connection.server.handler;

import io.zulia.message.ZuliaServiceOuterClass.GetIndexSettingsRequest;
import io.zulia.message.ZuliaServiceOuterClass.GetIndexSettingsResponse;
import io.zulia.server.index.ZuliaIndexManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GetIndexSettingsServerRequest extends ServerRequestHandler<GetIndexSettingsResponse, GetIndexSettingsRequest> {

	private final static Logger LOG = LoggerFactory.getLogger(GetIndexSettingsServerRequest.class.getSimpleName());

	public GetIndexSettingsServerRequest(ZuliaIndexManager indexManager) {
		super(indexManager);
	}

	@Override
	protected GetIndexSettingsResponse handleCall(ZuliaIndexManager indexManager, GetIndexSettingsRequest request) throws Exception {
		return indexManager.getIndexSettings(request);
	}

	@Override
	protected void onError(Throwable e) {
		LOG.error("Failed to handle internal get index settings", e);
	}
}
