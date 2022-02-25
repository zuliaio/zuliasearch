package io.zulia.server.search.queryparser;

import io.zulia.ZuliaConstants;
import io.zulia.message.ZuliaIndex;
import io.zulia.message.ZuliaIndex.FieldConfig.FieldType;
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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class ZuliaQueryParser {

	private final ServerIndexConfig indexConfig;
	private final ZuliaStandardQueryParser zuliaStandardQueryParser;

	private ZuliaIndex.IndexSettings indexSettings;

	public ZuliaQueryParser(Analyzer analyzer, ServerIndexConfig indexConfig) {
		zuliaStandardQueryParser = new ZuliaStandardQueryParser();
		zuliaStandardQueryParser.setAnalyzer(analyzer);
		this.indexConfig = indexConfig;
		zuliaStandardQueryParser.setAllowLeadingWildcard(true);

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

			if (field.contains("*")) {
				String regex = field.replace("*", ".*");
				Set<String> fieldNames = indexConfig.getMatchingFields(regex);
				allFields.addAll(fieldNames);

				if (boost != null) {
					for (String f : fieldNames) {
						boostMap.put(f, boost);
					}
				}
			}
			else {
				allFields.add(field);
				if (boost != null) {
					boostMap.put(field, boost);
				}
			}
		}

		zuliaStandardQueryParser.setMultiFields(allFields.toArray(new String[0]));
		zuliaStandardQueryParser.setFieldsBoost(boostMap);

	}

	public Query parse(String query) throws QueryNodeException {
		return zuliaStandardQueryParser.parse(query, null);
	}

	public void setMultiFields(List<String> fields) {
		zuliaStandardQueryParser.setMultiFields(fields.toArray(new String[0]));
	}

	public void setMinMatch(int minMatch) {
		zuliaStandardQueryParser.setMinMatch(minMatch);
	}
}
