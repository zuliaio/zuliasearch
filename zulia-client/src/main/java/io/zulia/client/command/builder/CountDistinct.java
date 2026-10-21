package io.zulia.client.command.builder;

import io.zulia.message.ZuliaQuery.DistinctCountRequest;

public class CountDistinct implements DistinctCountBuilder {

	private final DistinctCountRequest.Builder distinctCountRequestBuilder;

	public CountDistinct(String field) {
		distinctCountRequestBuilder = DistinctCountRequest.newBuilder().setField(field);
	}

	/**
	 * Distinct values one shard may send before the count becomes a floor. 0 uses the server default.
	 */
	public CountDistinct setMaxGroupsPerShard(int maxGroupsPerShard) {
		if (maxGroupsPerShard < 0) {
			throw new IllegalArgumentException(
					"maxGroupsPerShard for distinct count field <" + distinctCountRequestBuilder.getField() + "> must be 0 for the default or positive, got "
							+ maxGroupsPerShard);
		}
		distinctCountRequestBuilder.setMaxGroupsPerShard(maxGroupsPerShard);
		return this;
	}

	@Override
	public DistinctCountRequest getDistinctCountRequest() {
		return distinctCountRequestBuilder.build();
	}
}
