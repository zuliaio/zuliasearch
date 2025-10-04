package io.zulia.server.test.util;

import io.zulia.util.ZuliaVersion;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class VersionTest {

	@Test
	public void testVersionIsNotNull() {
		String version = ZuliaVersion.getVersion();
		Assertions.assertNotNull(version);
		Assertions.assertFalse(version.isEmpty());
		Assertions.assertTrue(version.startsWith("4."));
	}

}
