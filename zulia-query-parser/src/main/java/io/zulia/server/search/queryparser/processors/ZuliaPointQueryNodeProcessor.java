package io.zulia.server.search.queryparser.processors;

import io.zulia.ZuliaConstants;
import io.zulia.message.ZuliaIndex.FieldConfig.FieldType;
import io.zulia.server.config.ServerIndexConfig;
import io.zulia.server.field.FieldTypeUtil;
import io.zulia.server.search.queryparser.node.ZuliaPointRangeQueryNode;
import io.zulia.util.BooleanUtil;
import io.zulia.util.ZuliaDateUtil;
import org.apache.lucene.queryparser.flexible.core.nodes.FieldQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.MatchNoDocsQueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.nodes.RangeQueryNode;
import org.apache.lucene.queryparser.flexible.core.processors.QueryNodeProcessorImpl;
import org.apache.lucene.queryparser.flexible.core.util.StringUtils;
import org.apache.lucene.queryparser.flexible.standard.nodes.PointQueryNode;
import org.apache.lucene.queryparser.flexible.standard.nodes.TermRangeQueryNode;

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.List;
import java.util.Locale;

public class ZuliaPointQueryNodeProcessor extends QueryNodeProcessorImpl {

	/**
	 * Constructs a {@link ZuliaPointQueryNodeProcessor} object.
	 */
	public ZuliaPointQueryNodeProcessor() {
		// empty constructor
	}

	@Override
	protected QueryNode postProcessNode(QueryNode node) {

		if (node instanceof FieldQueryNode fieldNode && !(node.getParent() instanceof RangeQueryNode)) {

			ServerIndexConfig indexConfig = getQueryConfigHandler().get(ZuliaQueryNodeProcessorPipeline.ZULIA_INDEX_CONFIG);

			String field = fieldNode.getFieldAsString();
			System.out.println("Field: " + field);

			FieldType actualFieldType = indexConfig.getFieldTypeForIndexField(field);
			System.out.println("actualFieldType: " + actualFieldType);
			FieldType effectiveFieldType = getEffectiveFieldType(field, actualFieldType);

			if (FieldTypeUtil.isNumericFieldType(effectiveFieldType)) {

				String text = fieldNode.getTextAsString();

				NumberFormat numberFormat = FieldTypeUtil.isNumericFloatingPointFieldType(effectiveFieldType) ?
						NumberFormat.getNumberInstance(Locale.ROOT) :
						NumberFormat.getIntegerInstance(Locale.ROOT);

				Number number = parseNumber(text, numberFormat, actualFieldType);
				if (number == null) {
					return new MatchNoDocsQueryNode();
				}

				PointQueryNode lowerNode = new PointQueryNode(field, number, numberFormat);
				PointQueryNode upperNode = new PointQueryNode(field, number, numberFormat);

				String firstSortFieldFromIndexField = indexConfig.getFirstSortFieldFromIndexField(field);

				return new ZuliaPointRangeQueryNode(lowerNode, upperNode, true, true, effectiveFieldType, firstSortFieldFromIndexField);

			}

		}
		else if (node instanceof TermRangeQueryNode termRangeNode) {
			ServerIndexConfig indexConfig = getQueryConfigHandler().get(ZuliaQueryNodeProcessorPipeline.ZULIA_INDEX_CONFIG);

			String field = StringUtils.toString(termRangeNode.getField());

			FieldType actualFieldType = indexConfig.getFieldTypeForIndexField(field);
			FieldType effectiveFieldType = getEffectiveFieldType(field, actualFieldType);

			if (FieldTypeUtil.isNumericFieldType(effectiveFieldType)) {

				FieldQueryNode upper = termRangeNode.getUpperBound();
				FieldQueryNode lower = termRangeNode.getLowerBound();

				NumberFormat numberFormat = FieldTypeUtil.isNumericFloatingPointFieldType(effectiveFieldType) ?
						NumberFormat.getNumberInstance(Locale.ROOT) :
						NumberFormat.getIntegerInstance(Locale.ROOT);

				Number lowerNumber = parseNumber(lower.getTextAsString(), numberFormat, actualFieldType);
				if (lowerNumber == null && !lower.getTextAsString().isEmpty()) {
					return new MatchNoDocsQueryNode();
				}

				Number upperNumber = parseNumber(upper.getTextAsString(), numberFormat, actualFieldType);
				if (upperNumber == null && !upper.getTextAsString().isEmpty()) {
					return new MatchNoDocsQueryNode();
				}

				PointQueryNode lowerNode = new PointQueryNode(field, lowerNumber, numberFormat);
				PointQueryNode upperNode = new PointQueryNode(field, upperNumber, numberFormat);

				String firstSortFieldFromIndexField = indexConfig.getFirstSortFieldFromIndexField(field);

				return new ZuliaPointRangeQueryNode(lowerNode, upperNode, termRangeNode.isLowerInclusive(), termRangeNode.isUpperInclusive(),
						effectiveFieldType, firstSortFieldFromIndexField);

			}
		}
		return node;
	}

	private static Number parseNumber(String text, NumberFormat numberFormat, FieldType actualFieldType) {
		Number number = null;
		if (text.length() > 0) {

			if (FieldTypeUtil.isNumericFieldType(actualFieldType)) {

				try {
					number = numberFormat.parse(text);

					if (FieldTypeUtil.isNumericIntFieldType(actualFieldType)) {
						number = number.intValue();
					}
					else if (FieldTypeUtil.isNumericLongFieldType(actualFieldType)) {
						number = number.longValue();
					}
					else if (FieldTypeUtil.isNumericFloatFieldType(actualFieldType)) {
						number = number.floatValue();
					}
					else if (FieldTypeUtil.isNumericDoubleFieldType(actualFieldType)) {
						number = number.doubleValue();
					}
				}
				catch (ParseException e) {

				}
			}
			else if (FieldTypeUtil.isBooleanFieldType(actualFieldType)) {
				int booleanInt = BooleanUtil.getStringAsBooleanInt(text);
				number = booleanInt < 0 ? null : booleanInt;
			}
			else if (FieldTypeUtil.isDateFieldType(actualFieldType)) {
				number = ZuliaDateUtil.getDateAsLong(text);
			}
		}
		return number;
	}

	private static FieldType getEffectiveFieldType(String field, FieldType fieldType) {
		boolean lengthPrefix = field.startsWith(ZuliaConstants.CHAR_LENGTH_PREFIX) || field.startsWith(ZuliaConstants.LIST_LENGTH_PREFIX);
		if (lengthPrefix) {
			fieldType = FieldType.NUMERIC_INT;
		}
		else if (FieldTypeUtil.isDateFieldType(fieldType)) {
			fieldType = FieldType.NUMERIC_LONG;
		}
		else if (FieldTypeUtil.isBooleanFieldType(fieldType)) {
			fieldType = FieldType.NUMERIC_INT;
		}
		return fieldType;
	}

	@Override
	protected QueryNode preProcessNode(QueryNode node) {
		return node;
	}

	@Override
	protected List<QueryNode> setChildrenOrder(List<QueryNode> children) {
		return children;
	}

}
