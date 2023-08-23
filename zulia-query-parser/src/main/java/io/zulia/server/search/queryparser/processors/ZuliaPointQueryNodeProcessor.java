package io.zulia.server.search.queryparser.processors;

import io.zulia.message.ZuliaIndex.FieldConfig.FieldType;
import io.zulia.server.config.IndexFieldInfo;
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
import java.text.ParsePosition;
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
			IndexFieldInfo indexFieldInfo = indexConfig.getIndexFieldInfo(field);
			if (indexFieldInfo != null) {
				FieldType fieldType = indexFieldInfo.getFieldType();

				if (FieldTypeUtil.isHandledAsNumericFieldType(fieldType)) {

					String text = fieldNode.getTextAsString();

					NumberFormat numberFormat = FieldTypeUtil.isNumericFloatingPointFieldType(fieldType) ?
							NumberFormat.getNumberInstance(Locale.ROOT) :
							NumberFormat.getIntegerInstance(Locale.ROOT);

					Number number = parseNumber(text, numberFormat, fieldType);
					if (number == null) {
						return new MatchNoDocsQueryNode();
					}

					PointQueryNode lowerNode = new PointQueryNode(field, number, numberFormat);
					PointQueryNode upperNode = new PointQueryNode(field, number, numberFormat);

					return new ZuliaPointRangeQueryNode(lowerNode, upperNode, true, true, indexFieldInfo);

				}
			}

		}
		else if (node instanceof TermRangeQueryNode termRangeNode) {
			ServerIndexConfig indexConfig = getQueryConfigHandler().get(ZuliaQueryNodeProcessorPipeline.ZULIA_INDEX_CONFIG);

			String field = StringUtils.toString(termRangeNode.getField());
			IndexFieldInfo indexFieldInfo = indexConfig.getIndexFieldInfo(field);
			if (indexFieldInfo != null) {
				FieldType fieldType = indexFieldInfo.getFieldType();
				if (FieldTypeUtil.isHandledAsNumericFieldType(fieldType)) {

					FieldQueryNode upper = termRangeNode.getUpperBound();
					FieldQueryNode lower = termRangeNode.getLowerBound();

					NumberFormat numberFormat = FieldTypeUtil.isNumericFloatingPointFieldType(fieldType) ?
							NumberFormat.getNumberInstance(Locale.ROOT) :
							NumberFormat.getIntegerInstance(Locale.ROOT);

					Number lowerNumber = parseNumber(lower.getTextAsString(), numberFormat, fieldType);
					if (lowerNumber == null && !lower.getTextAsString().isEmpty()) {
						return new MatchNoDocsQueryNode();
					}

					Number upperNumber = parseNumber(upper.getTextAsString(), numberFormat, fieldType);
					if (upperNumber == null && !upper.getTextAsString().isEmpty()) {
						return new MatchNoDocsQueryNode();
					}

					PointQueryNode lowerNode = new PointQueryNode(field, lowerNumber, numberFormat);
					PointQueryNode upperNode = new PointQueryNode(field, upperNumber, numberFormat);

					return new ZuliaPointRangeQueryNode(lowerNode, upperNode, termRangeNode.isLowerInclusive(), termRangeNode.isUpperInclusive(),
							indexFieldInfo);

				}
			}
		}
		return node;
	}

	private static Number parseNumber(String text, NumberFormat numberFormat, FieldType fieldType) {
		Number number = null;
		if (!text.isEmpty()) {
			if (FieldTypeUtil.isNumericFieldType(fieldType)) {

				ParsePosition parsePosition = new ParsePosition(0);
				number = numberFormat.parse(text, parsePosition);
				if (parsePosition.getIndex() == text.length()) {

					if (FieldTypeUtil.isNumericIntFieldType(fieldType)) {
						number = number.intValue();
					}
					else if (FieldTypeUtil.isNumericLongFieldType(fieldType)) {
						number = number.longValue();
					}
					else if (FieldTypeUtil.isNumericFloatFieldType(fieldType)) {
						number = number.floatValue();
					}
					else if (FieldTypeUtil.isNumericDoubleFieldType(fieldType)) {
						number = number.doubleValue();
					}
				}
				else {
					number = null;
				}

			}
			else if (FieldTypeUtil.isBooleanFieldType(fieldType)) {
				number = BooleanUtil.getStringAsBooleanInt(text);
			}
			else if (FieldTypeUtil.isDateFieldType(fieldType)) {
				number = ZuliaDateUtil.getDateAsLong(text);
			}
		}
		return number;
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
