package io.zulia.server.connection;

import io.zulia.message.ZuliaServiceGrpc;
import io.zulia.server.index.ZuliaIndexManager;

public class ZuliaServiceHandler extends ZuliaServiceGrpc.ZuliaServiceImplBase {

	private final ZuliaIndexManager zuliaIndexManager;

	public ZuliaServiceHandler(ZuliaIndexManager zuliaIndexManager) {
		this.zuliaIndexManager = zuliaIndexManager;
	}

	//TODO implement
}
