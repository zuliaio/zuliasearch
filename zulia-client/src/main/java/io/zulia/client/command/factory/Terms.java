package io.zulia.client.command.factory;

import io.zulia.util.QueryHelper;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

public class Terms {

	private final Collection<String> fields;
	private Collection<String> terms;

	private boolean exclude;

	private Function<String, String> termHandler = QueryHelper.VALUE_QUOTER;

	public static Terms defaultFields() {
		return new Terms(List.of());
	}

	public static Terms withField(String field) {
		return new Terms(List.of(field));
	}

	public static Terms withFields(String... fields) {
		return new Terms(List.of(fields));
	}

	public static Terms withFields(Collection<String> fields) {
		return new Terms(fields);
	}

	private Terms(Collection<String> fields) {
		this.fields = fields;
	}

	public Terms of(String... terms) {
		this.terms = Arrays.stream(terms).toList();
		return this;
	}

	public Terms of(Collection<String> terms) {
		this.terms = terms;
		return this;
	}

	public Terms exclude() {
		this.exclude = true;
		return this;
	}

	public Terms include() {
		this.exclude = false;
		return this;
	}

	public Terms termHandler(Function<String, String> termHandler) {
		this.termHandler = termHandler;
		return this;
	}

	public Terms termHandlerChain(List<Function<String, String>> termHandlers) {
		this.termHandler = s -> {

			for (Function<String, String> handler : termHandlers) {
				s = handler.apply(s);
			}
			return s;
		};
		return this;
	}

	public String asString() {

		StringBuilder sb = new StringBuilder();

		if (exclude) {
			sb.append("-");
		}

		if (fields != null && !fields.isEmpty()) {
			QueryHelper.COMMA_JOINER.appendTo(sb, fields);
			sb.append(":");
		}
		sb.append("zl:tq");
		sb.append("(");

		List<String> valuesHandled = terms.stream().map(termHandler).toList();
		QueryHelper.SPACE_JOINER.appendTo(sb, valuesHandled);
		sb.append(")");

		return sb.toString();
	}

}
