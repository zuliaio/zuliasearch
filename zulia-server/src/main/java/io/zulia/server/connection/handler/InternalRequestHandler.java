package io.zulia.server.connection.handler;

import io.grpc.Metadata;
import io.grpc.StatusRuntimeException;
import io.zulia.cache.MetaKeys;
import io.zulia.message.ZuliaBase.Node;
import io.zulia.server.connection.InternalClient;
import io.zulia.server.connection.InternalRpcConnection;

public abstract class InternalRequestHandler<S, Q> {

	private InternalClient internalClient;

	public InternalRequestHandler(InternalClient internalClient) {
		this.internalClient = internalClient;
	}

	public S handleRequest(Node node, Q q) throws Exception {
		InternalRpcConnection rpcConnection = null;
		try {
			rpcConnection = internalClient.getInternalRpcConnection(node);

			S response = getResponse(q, rpcConnection);

			internalClient.returnInternalBlockingConnection(node, rpcConnection, true);
			return response;
		}
		catch (StatusRuntimeException e) {
			Metadata trailers = e.getTrailers();
			if (trailers.containsKey(MetaKeys.ERROR_KEY)) {
				throw new Exception(trailers.get(MetaKeys.ERROR_KEY));
			}
			else {
				throw e;
			}
		}
		catch (Exception e) {
			internalClient.returnInternalBlockingConnection(node, rpcConnection, false);
			throw e;
		}
	}

	protected abstract S getResponse(Q q, InternalRpcConnection rpcConnection);

}
