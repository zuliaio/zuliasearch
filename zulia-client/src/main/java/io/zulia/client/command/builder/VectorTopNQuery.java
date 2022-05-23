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

}
