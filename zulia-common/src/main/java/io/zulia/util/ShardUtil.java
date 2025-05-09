package io.zulia.util;

public class ShardUtil {
	public static int findShardForUniqueId(String uniqueId, int numOfShards) {
		return (int) Math.abs(djb2Hash(uniqueId)) % numOfShards;
	}

	public static long djb2Hash(String str) {

		long hash = 5381;

		for (int i = 0; i < str.length(); i++) {
			hash = ((hash << 5) + hash) + str.charAt(i);
		}

		return hash;
	}

}
