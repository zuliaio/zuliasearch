package io.zulia.server.search.queryparser;

import io.zulia.message.ZuliaQuery;
import org.apache.lucene.search.Query;

import java.util.Collection;

public interface ZuliaParser {

	void setDefaultFields(Collection<String> fields);

	void setMinimumNumberShouldMatch(int minimumNumberShouldMatch);

	void setDefaultOperator(ZuliaQuery.Query.Operator operator);

	Query parse(String query) throws Exception;

}
