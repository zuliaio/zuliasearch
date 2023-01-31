package io.zulia.util;

public class ShardUtil {
    public static int findShardForUniqueId(String uniqueId, int numOfShards) {
        return Math.abs(uniqueId.hashCode()) % numOfShards;
    }
}
