package io.zulia.server.test.node;

import org.testng.annotations.Test;

public class StartStopTest {

	public static final String FACET_TEST_INDEX = "plugged-54a725bc148f6dd7d62bc600";

	private final int COUNT_PER_ISSN = 10;
	private final String uniqueIdPrefix = "myId-";

	private final String[] issns = new String[] { "1234-1234", "3333-1234", "1234-5555", "1234-4444", "2222-2222" };
	private int totalRecords = COUNT_PER_ISSN * issns.length;

	public static void main(String[] args) throws Exception {
		TestHelper.startNodes(3);

		TestHelper.stopNodes();
	}

	@Test(dependsOnMethods="someBloodyMethod")
	public void testFieldExtraction() throws Exception {

	}
}
