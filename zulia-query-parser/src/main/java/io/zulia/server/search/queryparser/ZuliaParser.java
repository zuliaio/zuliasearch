package io.zulia.server.search.queryparser;

import com.google.common.base.Splitter;
import io.zulia.ZuliaConstants;
import io.zulia.message.ZuliaQuery;
import io.zulia.server.config.ServerIndexConfig;
import org.apache.lucene.search.Query;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public interface ZuliaParser {
	Splitter COMMA_SPLIT = Splitter.on(",").omitEmptyStrings();

	void setDefaultFields(Collection<String> fields);

	void setMinimumNumberShouldMatch(int minimumNumberShouldMatch);

	void setDefaultOperator(ZuliaQuery.Query.Operator operator);

	Query parse(String query) throws Exception;

	static String rewriteLengthFields(String field) {
		if (field.startsWith("|||") && field.endsWith("|||")) {
			field = ZuliaConstants.LIST_LENGTH_PREFIX + removeLengthBars(field);
		}
		else if (field.startsWith("||") && field.endsWith("||")) {
			// no-op
		}
		else if (field.startsWith("|") && field.endsWith("|")) {
			field = ZuliaConstants.CHAR_LENGTH_PREFIX + removeLengthBars(field);
		}
		return field;
	}

	static String removeLengthBars(String field) {
		if (field.startsWith("|||") && field.endsWith("|||")) {
			return field.substring(3, field.length() - 3);
		}
		else if (field.startsWith("||") && field.endsWith("||")) {
			// no-op
		}
		else if (field.startsWith("|") && field.endsWith("|")) {
			return field.substring(1, field.length() - 1);
		}
		return field;
	}

	static List<String> expandFields(ServerIndexConfig serverIndexConfig, CharSequence... fields) {
		return Arrays.stream(fields).flatMap(COMMA_SPLIT::splitToStream).flatMap(field -> serverIndexConfig.getMatchingFields(field).stream()).toList();
	}
}
