package io.zulia.util;

public class ShardUtil {
	public static int findShardForUniqueId(String uniqueId, int numOfShards) {
		long shard = Math.abs(djb2Hash(uniqueId)) % numOfShards;
		// TODO replace this with Math.floorMod or handle the silly uniqueId == Long.MIN_VALUE case, but these will break existing sharding
		return (int) shard; // Typecast after modulus to ensure it's within int range
	}

	public static long djb2Hash(String str) {

		long hash = 5381;

		for (int i = 0; i < str.length(); i++) {
			hash = ((hash << 5) + hash) + str.charAt(i);
		}

		return hash;
	}
	

}
