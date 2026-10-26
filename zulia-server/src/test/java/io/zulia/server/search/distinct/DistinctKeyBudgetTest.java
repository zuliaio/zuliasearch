package io.zulia.server.search.distinct;

import io.zulia.message.ZuliaQuery.DistinctCountInternal;
import io.zulia.message.ZuliaQuery.IndexShardResponse;
import io.zulia.message.ZuliaQuery.ShardQueryResponse;
import io.zulia.message.ZuliaServiceOuterClass.InternalQueryResponse;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;

public class DistinctKeyBudgetTest {

	@Test
	public void keysAreSummedAcrossShardsAndIndexes() {
		// two indexes, three shards, two fields, keys 3 + 2 + 4 + 1 = 10
		InternalQueryResponse response = InternalQueryResponse.newBuilder()
				.addIndexShardResponse(IndexShardResponse.newBuilder().addShardQueryResponse(shard(count("journal", 3), count("year", 2)))
						.addShardQueryResponse(shard(count("journal", 4)))).addIndexShardResponse(IndexShardResponse.newBuilder().addShardQueryResponse(shard(count("journal", 1))))
				.build();
		// far under the budget, so it passes however many shards there are
		Assertions.assertDoesNotThrow(() -> DistinctKeyBudget.check(response, "node a:1"));
	}

	@Test
	public void theBudgetIsTheMessageLimitLessHeadroom() {
		long limit = DistinctKeyBudget.MAX_KEY_BYTES_PER_MESSAGE;
		Assertions.assertEquals(224L * 1024 * 1024, limit);
		Assertions.assertDoesNotThrow(() -> DistinctKeyBudget.check(limit, Set.of("journal"), 30, "node a:1"));

		IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class,
				() -> DistinctKeyBudget.check(limit + 8, Set.of("journal", "year"), 30, "node a:1"));
		Assertions.assertTrue(e.getMessage().contains("journal") && e.getMessage().contains("year"), e.getMessage());
		Assertions.assertTrue(e.getMessage().contains("30 shards") && e.getMessage().contains("node a:1") && e.getMessage().contains("234 MB"), e.getMessage());
	}

	private static ShardQueryResponse shard(DistinctCountInternal... counts) {
		ShardQueryResponse.Builder builder = ShardQueryResponse.newBuilder();
		for (DistinctCountInternal count : counts) {
			builder.addDistinctCount(count);
		}
		return builder.build();
	}

	private static DistinctCountInternal count(String field, int keys) {
		DistinctCountInternal.Builder builder = DistinctCountInternal.newBuilder().setField(field).setCount(keys);
		for (int i = 0; i < keys; i++) {
			builder.addKey(i);
		}
		return builder.build();
	}
}
