package io.zulia.server.search;

import io.zulia.message.ZuliaQuery.StatGroup;
import io.zulia.message.ZuliaQuery.StatRequest;

import java.util.ArrayList;
import java.util.List;

public class StatCombiner {

	public static class StatGroupWithShardIndex {
		private final StatGroup statGroup;
		private final int shardIndex;

		public StatGroupWithShardIndex(StatGroup statGroup, int shardIndex) {
			this.statGroup = statGroup;
			this.shardIndex = shardIndex;
		}

		public StatGroup getStatGroup() {
			return statGroup;
		}

		public int getShardIndex() {
			return shardIndex;
		}
	}

	private final List<StatGroupWithShardIndex> statGroups;
	private final int[] shardIndexes;
	private final StatRequest statRequest;
	private final int shardReponses;

	public StatCombiner(StatRequest statRequest, int shardReponses) {
		this.statRequest = statRequest;
		this.shardReponses = shardReponses;
		this.statGroups = new ArrayList<>(shardReponses);
		this.shardIndexes = new int[shardReponses];
	}

	public void handleStatGroupForShard(StatGroup statGroup, int shardIndex) {
		statGroups.add(new StatGroupWithShardIndex(statGroup, shardIndex));
	}

	public StatGroup getCombinedStatGroup() {
		if (statGroups.size() == 1) {
			return statGroups.get(0).getStatGroup();
		}
		else {
			//TODO support this
			throw new UnsupportedOperationException("Multiple indexes or shards are not supported");
		}
	}
}
