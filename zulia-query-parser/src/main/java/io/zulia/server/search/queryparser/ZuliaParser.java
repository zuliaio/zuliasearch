package io.zulia.server.search.queryparser;

import io.zulia.ZuliaConstants;
import io.zulia.message.ZuliaQuery;
import org.apache.lucene.search.Query;

import java.util.Collection;

public interface ZuliaParser {
	void setDefaultFields(Collection<String> fields);

	void setMinimumNumberShouldMatch(int minimumNumberShouldMatch);

	void setDefaultOperator(ZuliaQuery.Query.Operator operator);

	Query parse(String query) throws Exception;

	static String rewriteLengthFields(String field) {
		if (field.startsWith("|||") && field.endsWith("|||")) {
			field = ZuliaConstants.LIST_LENGTH_PREFIX + field.substring(3, field.length() - 3);
		}
		else if (field.startsWith("||") && field.endsWith("||")) {
			// no-op
		}
		else if (field.startsWith("|") && field.endsWith("|")) {
			field = ZuliaConstants.CHAR_LENGTH_PREFIX + field.substring(1, field.length() - 1);
		}
		return field;
	}
}
