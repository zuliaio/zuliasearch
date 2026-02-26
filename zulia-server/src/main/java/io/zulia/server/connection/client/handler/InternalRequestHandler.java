package io.zulia.server.connection.client.handler;

import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import io.zulia.cache.MetaKeys;
import io.zulia.message.ZuliaBase.Node;
import io.zulia.server.connection.client.InternalClient;
import io.zulia.server.connection.client.InternalRpcConnection;

public abstract class InternalRequestHandler<S, Q> {

	private InternalClient internalClient;

	public InternalRequestHandler(InternalClient internalClient) {
		this.internalClient = internalClient;
	}

	public S handleRequest(Node node, Q q) throws Exception {
		try {
			InternalRpcConnection rpcConnection = internalClient.getConnection(node);
			return getResponse(q, rpcConnection);
		}
		catch (StatusRuntimeException e) {
			Metadata trailers = e.getTrailers();
			if (trailers != null && trailers.containsKey(MetaKeys.ERROR_KEY)) {
				throw new Exception(trailers.get(MetaKeys.ERROR_KEY));
			}
			else {
				throw e;
			}
		}
	}

	protected abstract S getResponse(Q q, InternalRpcConnection rpcConnection);

}
