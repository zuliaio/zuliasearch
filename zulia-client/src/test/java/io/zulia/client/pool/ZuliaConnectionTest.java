package io.zulia.client.pool;

import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.zulia.message.ZuliaBase.Node;
import io.zulia.message.ZuliaServiceOuterClass.GetNodesRequest;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ZuliaConnectionTest {

	@Test
	void callAfterCloseFailsAsUnavailable() {
		// the pool can close a connection that another thread already took from its map
		ZuliaConnection connection = new ZuliaConnection(Node.newBuilder().setServerAddress("localhost").setServicePort(1).build(), false);
		connection.open();
		connection.close();

		StatusRuntimeException e = assertThrows(StatusRuntimeException.class,
				() -> connection.getService().getNodes(GetNodesRequest.getDefaultInstance()));
		assertEquals(Status.Code.UNAVAILABLE, e.getStatus().getCode());
	}
}
