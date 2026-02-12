package io.zulia.client.command.builder;

import io.zulia.message.ZuliaQuery;

import java.util.Arrays;
import java.util.Collection;

public class TermQuery implements QueryBuilder {

	private final ZuliaQuery.Query.Builder builder;

	public TermQuery(String... field) {
		this(Arrays.asList(field));
	}

	public TermQuery(Collection<String> field) {
		if (field.isEmpty()) {
			throw new IllegalArgumentException("Field(s) must be not empty");
		}

		builder = ZuliaQuery.Query.newBuilder().addAllQf(field).setQueryType(ZuliaQuery.Query.QueryType.TERMS);
	}

	public TermQuery addTerm(String term) {
		builder.addTerm(term);
		return this;
	}

	public TermQuery addTerms(Iterable<String> term) {
		builder.addAllTerm(term);
		return this;
	}

	public TermQuery addTerms(String... terms) {
		for (String term : terms) {
			builder.addTerm(term);
		}
		return this;
	}

	public TermQuery exclude() {
		builder.setQueryType(ZuliaQuery.Query.QueryType.TERMS_NOT);
		return this;
	}

	public TermQuery include() {
		builder.setQueryType(ZuliaQuery.Query.QueryType.TERMS);
		return this;
	}

	public TermQuery clearTerms() {
		builder.clearTerm();
		return this;
	}

	@Override
	public ZuliaQuery.Query getQuery() {
		return builder.build();
	}

}
