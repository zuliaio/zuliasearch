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
		// Validate the version format (major.minor.*) rather than a hard-coded major so this does not break on each major bump.
		Assertions.assertTrue(version.matches("\\d+\\.\\d+\\..*"), "Unexpected version format: " + version);
		Assertions.assertTrue(ZuliaVersion.getMajor() >= 5);
	}

}
