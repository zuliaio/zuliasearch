package io.zulia.server.index.resident;

import io.zulia.server.config.ZuliaConfig;

public record TransientIndexPolicy(int maxLoadedIndexes, int idleTimeoutSeconds, boolean evictReplicated) {

	public static TransientIndexPolicy fromConfig(ZuliaConfig zuliaConfig) {
		return new TransientIndexPolicy(zuliaConfig.getTransientIndexCacheSize(), zuliaConfig.getTransientIndexIdleTimeoutSeconds(),
				zuliaConfig.isTransientIndexEvictReplicated());
	}

	public boolean enabled() {
		return maxLoadedIndexes > 0 || idleTimeoutSeconds > 0;
	}
}
