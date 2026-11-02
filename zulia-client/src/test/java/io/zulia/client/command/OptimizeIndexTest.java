package io.zulia.client.command;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class OptimizeIndexTest {

	@Test
	public void segmentTargetReachesTheRequest() {
		// unset sends 0, which the server reads as a merge to one segment per shard
		Assertions.assertEquals(0, new OptimizeIndex("books").getRequest().getMaxNumberOfSegments());
		Assertions.assertEquals("books", new OptimizeIndex("books").getRequest().getIndexName());

		OptimizeIndex optimizeIndex = new OptimizeIndex("books").setMaxNumberOfSegments(8);
		Assertions.assertEquals(8, optimizeIndex.getMaxNumberOfSegments());
		Assertions.assertEquals(8, optimizeIndex.getRequest().getMaxNumberOfSegments());
	}

	@Test
	public void negativeSegmentTargetIsRejected() {
		IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class, () -> new OptimizeIndex("books").setMaxNumberOfSegments(-1));
		Assertions.assertTrue(e.getMessage().contains("books") && e.getMessage().contains("-1"), e.getMessage());
	}
}
