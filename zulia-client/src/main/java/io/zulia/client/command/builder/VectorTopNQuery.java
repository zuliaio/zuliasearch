package io.zulia.client.command.builder;

import io.zulia.message.ZuliaQuery;

import java.util.Collection;
import java.util.List;

public class VectorTopNQuery implements QueryBuilder {

	private final ZuliaQuery.Query.Builder builder;

	public VectorTopNQuery(float[] vector, int topN, String... field) {
		this(vector, topN, List.of(field));

	}

	public VectorTopNQuery(float[] vector, int topN, Collection<String> field) {
		if (field.isEmpty()) {
			throw new IllegalArgumentException("Field(s) must be not empty");
		}

		builder = ZuliaQuery.Query.newBuilder().setQueryType(ZuliaQuery.Query.QueryType.VECTOR).addAllQf(field).setVectorTopN(topN);
		for (float v : vector) {
			builder.addVector(v);
		}
	}

	@Override
	public ZuliaQuery.Query getQuery() {
		return builder.build();
	}

	public VectorTopNQuery addPreFilterQuery(QueryBuilder queryBuilder) {
		builder.addVectorPreQuery(queryBuilder.getQuery());
		return this;
	}

	public float getBoost() {
		return builder.getBoost();
	}

	public VectorTopNQuery setBoost(float boost) {
		builder.setBoost(boost);
		return this;
	}

	public boolean isMust() {
		return builder.getQueryType() == ZuliaQuery.Query.QueryType.VECTOR;
	}

	/**
	 * If true (default), the vector clause is required and constrains the result set to HNSW top-K matches.
	 * If false, the vector clause contributes score but does not restrict matches - suitable for hybrid
	 * retrieval alongside a SCORE_SHOULD clause.
	 */
	public VectorTopNQuery setMust(boolean must) {
		builder.setQueryType(must ? ZuliaQuery.Query.QueryType.VECTOR : ZuliaQuery.Query.QueryType.VECTOR_SHOULD);
		return this;
	}

}
