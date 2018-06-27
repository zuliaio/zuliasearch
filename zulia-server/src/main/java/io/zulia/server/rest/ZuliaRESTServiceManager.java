package io.zulia.server.rest;

import io.micronaut.runtime.Micronaut;
import io.zulia.server.config.ZuliaConfig;
import io.zulia.server.index.ZuliaIndexManager;

import javax.inject.Singleton;

public class ZuliaRESTServiceManager {

	@Singleton
	private final ZuliaIndexManager indexManager;

	public ZuliaRESTServiceManager(ZuliaConfig zuliaConfig, ZuliaIndexManager indexManager) {
		this.indexManager = indexManager;
	}

	public static void main(String[] args) {
		Micronaut.run(ZuliaRESTServiceManager.class);
	}
}
