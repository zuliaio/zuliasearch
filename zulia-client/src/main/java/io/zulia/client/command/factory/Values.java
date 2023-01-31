package io.zulia.client.command.factory;

import com.google.common.base.Joiner;
import io.zulia.client.command.builder.FilterQuery;
import io.zulia.client.command.builder.ScoredQuery;
import io.zulia.client.command.builder.StandardQuery;
import io.zulia.message.ZuliaQuery.Query.Operator;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

public class Values {
    public static final Joiner COMMA_JOINER = Joiner.on(",");
    public static final Joiner SPACE_JOINER = Joiner.on(" ");
    public static final Joiner OR_JOINER = Joiner.on(" OR ");

    public static final Joiner AND_JOINER = Joiner.on(" AND ");

    public static Function<String, String> VALUE_QUOTER = s -> {
        s = s.trim();
        if (s.startsWith("\"") && s.endsWith("\"")) {
            return s;
        }
        if (s.contains(" ") || s.contains("-")) {
            return "\"" + s + "\"";
        }
        return s;
    };


    private final Type type;

    private final Integer minimumShouldMatch;

    private Collection<String> values;

    private Collection<String> fields;
    private boolean exclude;

    private Function<String, String> valueHandler = VALUE_QUOTER;


    private enum Type {
        ANY, ALL, AT_LEAST
    }

    public static Values any() {
        return new Values(Type.ANY, null);
    }

    public static Values all() {
        return new Values(Type.ALL, null);
    }

    public static Values atLeast(int minimumShouldMatch) {
        return new Values(Type.AT_LEAST, minimumShouldMatch);
    }

    private Values(Type type, Integer minimumShouldMatch) {
        this.type = type;
        this.minimumShouldMatch = minimumShouldMatch;
    }

    public Values of(String... values) {
        this.values = Arrays.stream(values).toList();
        return this;
    }

    public Values of(Collection<String> values) {
        this.values = values;
        return this;
    }


    public Values exclude() {
        this.exclude = true;
        return this;
    }

    public Values include() {
        this.exclude = false;
        return this;
    }


    public Values withFields(String... fields) {
        this.fields = List.of(fields);
        return this;
    }

    public Values withFields(Collection<String> fields) {
        this.fields = fields;
        return this;
    }


    public Values valueHandler(Function<String, String> valueHandler) {
        this.valueHandler = valueHandler;
        return this;
    }

    public String asString() {

        StringBuilder sb = new StringBuilder();

        if (exclude) {
            sb.append("-");
        }

        if (fields != null && !fields.isEmpty()) {
            COMMA_JOINER.appendTo(sb, fields);
            sb.append(":");
        }
        sb.append("(");

        List<String> valuesHandled = values.stream().map(valueHandler).toList();
        if (type.equals(Type.ALL)) {
            AND_JOINER.appendTo(sb, valuesHandled);
        } else {
            OR_JOINER.appendTo(sb, valuesHandled);
        }
        sb.append(")");
        if (type.equals(Type.AT_LEAST)) {
            sb.append("~");
            sb.append(minimumShouldMatch);
        }
        return sb.toString();
    }


    public FilterQuery asFilterQuery() {
        return asQuery(FilterQuery::new);
    }

    public ScoredQuery asScoredQuery() {
        return asQuery(ScoredQuery::new);
    }


    public <T extends StandardQuery<T>> T asQuery(Function<String, T> constructor) {
        List<String> valuesHandled = values.stream().map(valueHandler).toList();
        String query = SPACE_JOINER.join(valuesHandled);
        T tQuery = constructor.apply(query);
        tQuery.setDefaultOperator(type.equals(Type.ALL) ? Operator.AND : Operator.OR);
        fields.forEach(tQuery::addQueryField);

        if (exclude) {
            if (tQuery instanceof FilterQuery fq) {
                fq.exclude();
            } else if (tQuery instanceof ScoredQuery sq) {
                throw new IllegalStateException("Exclude cannot be used with ScoredQuery");
            }
        }
        if (minimumShouldMatch != null) {
            tQuery.setMinShouldMatch(minimumShouldMatch);
        }
        return tQuery;
    }


}
