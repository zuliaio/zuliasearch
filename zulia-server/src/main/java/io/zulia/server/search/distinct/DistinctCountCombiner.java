package io.zulia.server.search.distinct;

import io.zulia.message.ZuliaIndex.FieldConfig.FieldType;
import io.zulia.message.ZuliaIndex.SortAs.StringHandling;
import io.zulia.message.ZuliaQuery.DistinctCount;
import io.zulia.message.ZuliaQuery.DistinctCountInternal;
import io.zulia.message.ZuliaQuery.DistinctCountRequest;
import io.zulia.message.ZuliaQuery.ShardQueryResponse;
import org.apache.lucene.search.TaskExecutor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Unions the sorted keys every shard returned for one distinct count request into a single count with the same pairwise
 * merge the shards use, counting the last level instead of writing it. The count is exact unless a shard was capped, in
 * which case the union of what every shard did send and the capped shards' own counts are both floors and the larger one
 * is reported.
 */
public final class DistinctCountCombiner {

	private DistinctCountCombiner() {
	}

	public static DistinctCount combine(int requestIndex, DistinctCountRequest request, List<ShardQueryResponse> shardResponses, TaskExecutor taskExecutor)
			throws IOException {
		String field = request.getField();
		List<DistinctCountInternal> shardKeys = new ArrayList<>(shardResponses.size());
		long floor = 0;
		boolean capped = false;
		FieldType fieldType = null;
		StringHandling stringHandling = null;
		String fieldTypeSource = null;

		for (ShardQueryResponse sr : shardResponses) {
			String shard = "Shard " + sr.getShardNumber() + " of index " + sr.getIndexName();
			if (sr.getDistinctCountCount() <= requestIndex) {
				throw new IllegalStateException(
						shard + " returned no distinct count for field <" + field + ">. It may be running a version without distinct counts");
			}
			DistinctCountInternal internal = sr.getDistinctCount(requestIndex);
			if (!field.equals(internal.getField())) {
				throw new IllegalStateException(shard + " returned distinct count for field <" + internal.getField() + "> where <" + field + "> was expected");
			}
			if (fieldType == null) {
				fieldType = internal.getFieldType();
				stringHandling = internal.getStringHandling();
				fieldTypeSource = shard;
			}
			else if (fieldType != internal.getFieldType()) {
				throw new IllegalStateException(
						"Distinct count field <" + field + "> is a " + internal.getFieldType() + " field on " + shard + " but a " + fieldType + " field on "
								+ fieldTypeSource + ". The field must have the same type in every searched index");
			}
			else if (stringHandling != internal.getStringHandling()) {
				// the keys are hashes of the normalized sort values, so differently folded indexes would count one journal twice
				throw new IllegalStateException(
						"Distinct count field <" + field + "> sorts with " + internal.getStringHandling() + " string handling on " + shard + " but "
								+ stringHandling + " on " + fieldTypeSource + ". The field must fold the same way in every searched index");
			}
			if (internal.getCapped()) {
				capped = true;
				floor = Math.max(floor, internal.getCount());
			}
			if (internal.getKeyCount() > 0) {
				shardKeys.add(internal);
			}
		}

		// The key arrays are built inside the call so the merge is their only holder and can let them go after its first level
		long count = Math.max(SortedLongUnion.countDistinct(toArrays(shardKeys), taskExecutor), floor);
		return DistinctCount.newBuilder().setField(field).setCount(count).setExact(!capped).build();
	}

	private static List<long[]> toArrays(List<DistinctCountInternal> shardKeys) {
		List<long[]> arrays = new ArrayList<>(shardKeys.size());
		for (DistinctCountInternal internal : shardKeys) {
			arrays.add(toArray(internal));
		}
		return arrays;
	}

	private static long[] toArray(DistinctCountInternal internal) {
		long[] keys = new long[internal.getKeyCount()];
		for (int i = 0; i < keys.length; i++) {
			keys[i] = internal.getKey(i);
		}
		return keys;
	}
}
