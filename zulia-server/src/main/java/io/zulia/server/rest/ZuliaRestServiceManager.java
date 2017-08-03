package io.zulia.server.rest;

import io.zulia.server.config.ZuliaConfig;
import io.zulia.server.index.ZuliaIndexManager;

public class ZuliaRestServiceManager {

	private final int restPort;
	private final ZuliaIndexManager zuliaIndexManager;

	public ZuliaRestServiceManager(ZuliaConfig zuliaConfig, ZuliaIndexManager zuliaIndexManager) {
		this.restPort = zuliaConfig.getRestPort();
		this.zuliaIndexManager = zuliaIndexManager;
	}

	public void start() {

	}

	public void stop() {

	}
}
