package io.zulia.server.search.distinct;

import io.zulia.message.ZuliaIndex.FieldConfig.FieldType;
import io.zulia.message.ZuliaIndex.SortAs.StringHandling;
import io.zulia.message.ZuliaQuery.DistinctCount;
import io.zulia.message.ZuliaQuery.DistinctCountInternal;
import io.zulia.message.ZuliaQuery.DistinctCountRequest;
import io.zulia.message.ZuliaQuery.ShardQueryResponse;
import org.apache.lucene.search.TaskExecutor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class DistinctCountCombinerTest {

	private static final DistinctCountRequest JOURNAL = DistinctCountRequest.newBuilder().setField("journal").build();
	private static final TaskExecutor INLINE = new TaskExecutor(Runnable::run);

	@Test
	public void combineUnionsShardsAndReportsExact() throws Exception {
		List<ShardQueryResponse> shards = List.of(shardResponseWithDistinct(0, List.of(1L, 2L, 3L), 3, false),
				shardResponseWithDistinct(1, List.of(3L, 4L), 2, false), shardResponseWithDistinct(2, List.of(), 0, false));
		DistinctCount result = DistinctCountCombiner.combine(0, JOURNAL, shards, INLINE);
		Assertions.assertEquals("journal", result.getField());
		Assertions.assertEquals(4, result.getCount());
		Assertions.assertTrue(result.getExact());
	}

	@Test
	public void combineReportsAFloorWhenAnyShardIsCapped() throws Exception {
		List<ShardQueryResponse> shards = List.of(shardResponseWithDistinct(0, List.of(1L, 2L, 3L), 3, false),
				shardResponseWithDistinct(1, List.of(), 10, true), shardResponseWithDistinct(2, List.of(3L, 4L), 2, false));
		DistinctCount result = DistinctCountCombiner.combine(0, JOURNAL, shards, INLINE);
		Assertions.assertEquals(10, result.getCount(), "the highest capped floor wins over a smaller union");
		Assertions.assertFalse(result.getExact());

		List<ShardQueryResponse> smallFloor = List.of(shardResponseWithDistinct(0, List.of(1L, 2L, 3L, 4L, 5L), 5, false),
				shardResponseWithDistinct(1, List.of(), 2, true));
		result = DistinctCountCombiner.combine(0, JOURNAL, smallFloor, INLINE);
		Assertions.assertEquals(5, result.getCount(), "the union wins over a smaller capped floor");
		Assertions.assertFalse(result.getExact());

		// a capped shard ships its first keys, and values the other shards do not hold raise the floor above either number alone
		List<ShardQueryResponse> partialKeys = List.of(shardResponseWithDistinct(0, List.of(1L, 2L, 3L, 4L, 5L), 5, false),
				shardResponseWithDistinct(1, List.of(2L, 6L, 7L), 8, true));
		result = DistinctCountCombiner.combine(0, JOURNAL, partialKeys, INLINE);
		Assertions.assertEquals(8, result.getCount(), "the capped shard's own count is the larger floor here");
		List<ShardQueryResponse> unionWins = List.of(shardResponseWithDistinct(0, List.of(1L, 2L, 3L, 4L, 5L), 5, false),
				shardResponseWithDistinct(1, List.of(6L, 7L, 8L), 4, true));
		result = DistinctCountCombiner.combine(0, JOURNAL, unionWins, INLINE);
		Assertions.assertEquals(8, result.getCount(), "five plus three unseen values beat a capped count of four");
		Assertions.assertFalse(result.getExact());
	}

	@Test
	public void combineRejectsAMissingOrMismatchedShardEntry() {
		ShardQueryResponse noEntry = ShardQueryResponse.newBuilder().setShardNumber(1).setIndexName("articles").build();
		Assertions.assertThrows(Exception.class, () -> DistinctCountCombiner.combine(0, JOURNAL, List.of(noEntry), INLINE));

		ShardQueryResponse wrongField = ShardQueryResponse.newBuilder().setShardNumber(1).setIndexName("articles")
				.addDistinctCount(DistinctCountInternal.newBuilder().setField("year").setCount(1).addKey(2015)).build();
		Assertions.assertThrows(Exception.class, () -> DistinctCountCombiner.combine(0, JOURNAL, List.of(wrongField), INLINE));
	}

	@Test
	public void singleShardCountsItsOwnKeys() throws Exception {
		List<ShardQueryResponse> one = List.of(shardResponseWithDistinct(0, List.of(7L, 9L, 11L), 3, false));
		DistinctCount result = DistinctCountCombiner.combine(0, JOURNAL, one, INLINE);
		Assertions.assertEquals(3, result.getCount());
		Assertions.assertTrue(result.getExact());
	}

	@Test
	public void combineRejectsMixedFieldTypesAcrossIndexes() {
		ShardQueryResponse stringShard = ShardQueryResponse.newBuilder().setShardNumber(0).setIndexName("articlesA")
				.addDistinctCount(DistinctCountInternal.newBuilder().setField("journal").setCount(1).addKey(5).setFieldType(FieldType.STRING)).build();
		ShardQueryResponse doubleShard = ShardQueryResponse.newBuilder().setShardNumber(0).setIndexName("articlesB")
				.addDistinctCount(DistinctCountInternal.newBuilder().setField("journal").setCount(1).addKey(5).setFieldType(FieldType.NUMERIC_DOUBLE)).build();
		IllegalStateException e = Assertions.assertThrows(IllegalStateException.class,
				() -> DistinctCountCombiner.combine(0, JOURNAL, List.of(stringShard, doubleShard), INLINE));
		Assertions.assertTrue(e.getMessage().contains("articlesA") && e.getMessage().contains("articlesB"), e.getMessage());
		Assertions.assertTrue(e.getMessage().contains("NUMERIC_DOUBLE") && e.getMessage().contains("STRING"), e.getMessage());
	}

	@Test
	public void combineRejectsMixedStringHandlingAcrossIndexes() {
		ShardQueryResponse rawShard = ShardQueryResponse.newBuilder().setShardNumber(0).setIndexName("articlesA").addDistinctCount(
				DistinctCountInternal.newBuilder().setField("journal").setCount(1).addKey(5).setFieldType(FieldType.STRING)
						.setStringHandling(StringHandling.STANDARD)).build();
		ShardQueryResponse foldedShard = ShardQueryResponse.newBuilder().setShardNumber(0).setIndexName("articlesB").addDistinctCount(
				DistinctCountInternal.newBuilder().setField("journal").setCount(1).addKey(5).setFieldType(FieldType.STRING)
						.setStringHandling(StringHandling.LOWERCASE)).build();
		IllegalStateException e = Assertions.assertThrows(IllegalStateException.class,
				() -> DistinctCountCombiner.combine(0, JOURNAL, List.of(rawShard, foldedShard), INLINE));
		Assertions.assertTrue(e.getMessage().contains("articlesA") && e.getMessage().contains("articlesB"), e.getMessage());
		Assertions.assertTrue(e.getMessage().contains("LOWERCASE") && e.getMessage().contains("STANDARD"), e.getMessage());
	}

	private static ShardQueryResponse shardResponseWithDistinct(int shardNumber, List<Long> sortedKeys, long count, boolean capped) {
		DistinctCountInternal.Builder internal = DistinctCountInternal.newBuilder().setField("journal").setCount(count).setCapped(capped).addAllKey(sortedKeys);
		return ShardQueryResponse.newBuilder().setShardNumber(shardNumber).setIndexName("articles").addDistinctCount(internal).build();
	}
}
