package io.zulia.server.search.queryparser;

import io.zulia.message.ZuliaQuery;
import io.zulia.server.config.ServerIndexConfig;
import io.zulia.server.search.queryparser.processors.ZuliaQueryNodeProcessorPipeline;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler;
import org.apache.lucene.search.Query;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class ZuliaFlexibleQueryParser implements ZuliaParser {

	private final ServerIndexConfig indexConfig;
	private final ZuliaStandardQueryParser zuliaStandardQueryParser;

	public ZuliaFlexibleQueryParser(Analyzer analyzer, ServerIndexConfig indexConfig) {
		zuliaStandardQueryParser = new ZuliaStandardQueryParser(analyzer);
		zuliaStandardQueryParser.setAnalyzer(analyzer);
		this.indexConfig = indexConfig;
		zuliaStandardQueryParser.setAllowLeadingWildcard(true);
		zuliaStandardQueryParser.getQueryConfigHandler().set(ZuliaQueryNodeProcessorPipeline.ZULIA_INDEX_CONFIG, indexConfig);
	}

	@Override
	public void setDefaultFields(Collection<String> fields) {

		Map<String, Float> boostMap = new HashMap<>();
		Set<String> allFields = new TreeSet<>();
		for (String field : fields) {

			Float boost = null;
			if (field.contains("^")) {
				boost = Float.parseFloat(field.substring(field.indexOf("^") + 1));
				try {
					field = field.substring(0, field.indexOf("^"));

				}
				catch (Exception e) {
					throw new IllegalArgumentException("Invalid queryText field boost <" + field + ">");
				}
			}

			Set<String> fieldNames = indexConfig.getMatchingFields(field);
			allFields.addAll(fieldNames);

			if (boost != null) {
				for (String f : fieldNames) {
					boostMap.put(f, boost);
				}
			}

		}

		zuliaStandardQueryParser.setMultiFields(allFields.toArray(new String[0]));
		zuliaStandardQueryParser.setFieldsBoost(boostMap);

	}

	@Override
	public Query parse(String query) throws QueryNodeException {
		return zuliaStandardQueryParser.parse(query, null);
	}

	@Override
	public void setMinimumNumberShouldMatch(int minimumNumberShouldMatch) {
		zuliaStandardQueryParser.setMinMatch(minimumNumberShouldMatch);
	}

	@Override
	public void setDefaultOperator(ZuliaQuery.Query.Operator operator) {
		if (ZuliaQuery.Query.Operator.AND.equals(operator)) {
			zuliaStandardQueryParser.setDefaultOperator(StandardQueryConfigHandler.Operator.AND);
		}
		else if (ZuliaQuery.Query.Operator.OR.equals(operator)) {
			zuliaStandardQueryParser.setDefaultOperator(StandardQueryConfigHandler.Operator.OR);
		}
		else {
			throw new IllegalArgumentException("Operator must be AND or OR");
		}
	}
}
