package io.zulia.util;

public class ShardUtil {
	public static int findSegmentForUniqueId(String uniqueId, int numSegments) {
		int shardNumber = Math.abs(uniqueId.hashCode()) % numSegments;
		return shardNumber;
	}
}
