package io.zulia.client.command.builder;

import io.zulia.message.ZuliaQuery;

import java.util.Arrays;
import java.util.Collection;

public class VectorSimQuery implements QueryBuilder {

	private final ZuliaQuery.Query.Builder builder;

	public VectorSimQuery(double[] vector, double similarity, String... field) {
		this(vector, similarity, Arrays.asList(field));

	}

	public VectorSimQuery(double[] vector, double similarity, Collection<String> field) {
		if (field.isEmpty()) {
			throw new IllegalArgumentException("Field(s) must be not empty");
		}

		builder = ZuliaQuery.Query.newBuilder().addAllQf(field).setVectorSimilarity(similarity);
		for (int i = 0; i < vector.length; i++) {
			builder.addVector(vector[i]);
		}
	}

	@Override
	public ZuliaQuery.Query getQuery() {

		return builder.build();
	}
}
