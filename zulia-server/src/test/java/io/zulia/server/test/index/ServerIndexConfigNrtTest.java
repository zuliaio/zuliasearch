package io.zulia.server.test.index;

import io.zulia.message.ZuliaIndex.IndexSettings;
import io.zulia.server.config.ServerIndexConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for the NRT cache-size getters on ServerIndexConfig: unset (0) resolves to the default at use time
 * (the ramBufferMB pattern), explicit values pass through, and an in-place reconfigure (what reloadIndexSettings
 * does) is reflected by the getters - which is what lets ZuliaNRTCachingDirectory pick up live changes.
 */
public class ServerIndexConfigNrtTest {

	@Test
	public void defaultsWhenUnset() {
		ServerIndexConfig config = new ServerIndexConfig(IndexSettings.newBuilder().setIndexName("x").build());

		Assertions.assertEquals(ServerIndexConfig.DEFAULT_NRT_INDEX_MAX_MERGE_SIZE_MB, config.getNrtIndexMaxMergeSizeMB());
		Assertions.assertEquals(ServerIndexConfig.DEFAULT_NRT_INDEX_MAX_CACHED_MB, config.getNrtIndexMaxCachedMB());
		Assertions.assertEquals(ServerIndexConfig.DEFAULT_NRT_TAXO_MAX_MERGE_SIZE_MB, config.getNrtTaxoMaxMergeSizeMB());
		Assertions.assertEquals(ServerIndexConfig.DEFAULT_NRT_TAXO_MAX_CACHED_MB, config.getNrtTaxoMaxCachedMB());
		Assertions.assertFalse(config.isNrtCachingDisabled());
	}

	@Test
	public void usesExplicitValues() {
		IndexSettings settings = IndexSettings.newBuilder()
				.setIndexName("x")
				.setNrtIndexMaxMergeSizeMB(10)
				.setNrtIndexMaxCachedMB(20)
				.setNrtTaxoMaxMergeSizeMB(2)
				.setNrtTaxoMaxCachedMB(4)
				.setNrtCachingDisabled(true)
				.build();

		ServerIndexConfig config = new ServerIndexConfig(settings);

		Assertions.assertEquals(10, config.getNrtIndexMaxMergeSizeMB());
		Assertions.assertEquals(20, config.getNrtIndexMaxCachedMB());
		Assertions.assertEquals(2, config.getNrtTaxoMaxMergeSizeMB());
		Assertions.assertEquals(4, config.getNrtTaxoMaxCachedMB());
		Assertions.assertTrue(config.isNrtCachingDisabled());
	}

	@Test
	public void reconfigureInPlaceUpdatesGetters() {
		ServerIndexConfig config = new ServerIndexConfig(IndexSettings.newBuilder().setIndexName("x").build());
		Assertions.assertEquals(ServerIndexConfig.DEFAULT_NRT_INDEX_MAX_CACHED_MB, config.getNrtIndexMaxCachedMB());
		Assertions.assertFalse(config.isNrtCachingDisabled());

		// configure() replaces the backing settings on the same instance - this is what reloadIndexSettings calls.
		config.configure(IndexSettings.newBuilder().setIndexName("x").setNrtIndexMaxCachedMB(64).setNrtCachingDisabled(true).build());

		Assertions.assertEquals(64, config.getNrtIndexMaxCachedMB());
		Assertions.assertTrue(config.isNrtCachingDisabled());
	}
}
