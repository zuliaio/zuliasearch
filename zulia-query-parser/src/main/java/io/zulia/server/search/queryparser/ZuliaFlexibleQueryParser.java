package io.zulia.server.search.queryparser;

import io.zulia.ZuliaConstants;
import io.zulia.message.ZuliaIndex.FieldConfig.FieldType;
import io.zulia.message.ZuliaQuery;
import io.zulia.server.config.ServerIndexConfig;
import io.zulia.server.field.FieldTypeUtil;
import io.zulia.server.search.queryparser.processors.ZuliaQueryNodeProcessorPipeline;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.standard.config.PointsConfig;
import org.apache.lucene.queryparser.flexible.standard.config.StandardQueryConfigHandler;
import org.apache.lucene.search.Query;

import java.text.NumberFormat;
import java.util.Collection;
import java.util.HashMap;
import java.util.Locale;
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

		zuliaStandardQueryParser.getQueryConfigHandler().addFieldConfigListener(fieldConfig -> {

			String field = fieldConfig.getField();
			FieldType fieldType = indexConfig.getFieldTypeForIndexField(field);

			boolean lengthPrefix = field.startsWith(ZuliaConstants.CHAR_LENGTH_PREFIX) || field.startsWith(ZuliaConstants.LIST_LENGTH_PREFIX);
			if (lengthPrefix) {
				PointsConfig pointsConfig = new PointsConfig(NumberFormat.getIntegerInstance(Locale.ROOT), Integer.class);
				fieldConfig.set(StandardQueryConfigHandler.ConfigurationKeys.POINTS_CONFIG, pointsConfig);
			}
			else if (FieldTypeUtil.isDateFieldType(fieldType)) {
				PointsConfig pointsConfig = new PointsConfig(NumberFormat.getIntegerInstance(Locale.ROOT), Long.class);
				fieldConfig.set(StandardQueryConfigHandler.ConfigurationKeys.POINTS_CONFIG, pointsConfig);
			}
			else if (FieldTypeUtil.isNumericFieldType(fieldType)) {
				PointsConfig pointsConfig = null;
				if (FieldTypeUtil.isNumericIntFieldType(fieldType)) {
					pointsConfig = new PointsConfig(NumberFormat.getIntegerInstance(Locale.ROOT), Integer.class);
				}
				else if (FieldTypeUtil.isNumericLongFieldType(fieldType)) {
					pointsConfig = new PointsConfig(NumberFormat.getIntegerInstance(Locale.ROOT), Long.class);
				}
				else if (FieldTypeUtil.isNumericFloatFieldType(fieldType)) {
					pointsConfig = new PointsConfig(NumberFormat.getNumberInstance(Locale.ROOT), Float.class);
				}
				else if (FieldTypeUtil.isNumericDoubleFieldType(fieldType)) {
					pointsConfig = new PointsConfig(NumberFormat.getNumberInstance(Locale.ROOT), Double.class);
				}
				fieldConfig.set(StandardQueryConfigHandler.ConfigurationKeys.POINTS_CONFIG, pointsConfig);
			}

			fieldConfig.set(ZuliaQueryNodeProcessorPipeline.ZULIA_FIELD_TYPE, fieldType);

		});

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
