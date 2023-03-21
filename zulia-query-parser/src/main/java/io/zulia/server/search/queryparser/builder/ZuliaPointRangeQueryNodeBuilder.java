package io.zulia.server.search.queryparser.builder;

import io.zulia.message.ZuliaIndex.FieldConfig.FieldType;
import io.zulia.server.field.FieldTypeUtil;
import io.zulia.server.search.queryparser.node.ZuliaPointRangeQueryNode;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.messages.QueryParserMessages;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.core.util.StringUtils;
import org.apache.lucene.queryparser.flexible.messages.MessageImpl;
import org.apache.lucene.queryparser.flexible.standard.builders.StandardQueryBuilder;
import org.apache.lucene.queryparser.flexible.standard.nodes.PointQueryNode;
import org.apache.lucene.search.Query;

public class ZuliaPointRangeQueryNodeBuilder implements StandardQueryBuilder {

	/**
	 * Constructs a {@link ZuliaPointRangeQueryNodeBuilder} object.
	 */
	public ZuliaPointRangeQueryNodeBuilder() {
		// empty constructor
	}

	@Override
	public Query build(QueryNode queryNode) throws QueryNodeException {
		ZuliaPointRangeQueryNode numericRangeNode = (ZuliaPointRangeQueryNode) queryNode;

		PointQueryNode lowerNumericNode = numericRangeNode.getLowerBound();
		PointQueryNode upperNumericNode = numericRangeNode.getUpperBound();

		Number lowerNumber = lowerNumericNode.getValue();
		Number upperNumber = upperNumericNode.getValue();

		FieldType fieldType = numericRangeNode.getFieldConfig();

		String field = StringUtils.toString(numericRangeNode.getField());
		boolean minInclusive = numericRangeNode.isLowerInclusive();
		boolean maxInclusive = numericRangeNode.isUpperInclusive();

		if (FieldTypeUtil.isNumericIntFieldType(fieldType)) {
			Integer lower = (Integer) lowerNumber;
			if (lower == null) {
				lower = Integer.MIN_VALUE;
			}
			if (!minInclusive) {
				lower = lower + 1;
			}

			Integer upper = (Integer) upperNumber;
			if (upper == null) {
				upper = Integer.MAX_VALUE;
			}
			if (!maxInclusive) {
				upper = upper - 1;
			}
			return IntPoint.newRangeQuery(field, lower, upper);
		}
		else if (FieldTypeUtil.isNumericLongFieldType(fieldType)) {
			Long lower = (Long) lowerNumber;
			if (lower == null) {
				lower = Long.MIN_VALUE;
			}
			if (!minInclusive) {
				lower = lower + 1;
			}

			Long upper = (Long) upperNumber;
			if (upper == null) {
				upper = Long.MAX_VALUE;
			}
			if (!maxInclusive) {
				upper = upper - 1;
			}
			return LongPoint.newRangeQuery(field, lower, upper);
		}
		else if (FieldTypeUtil.isNumericFloatFieldType(fieldType)) {
			Float lower = (Float) lowerNumber;
			if (lower == null) {
				lower = Float.NEGATIVE_INFINITY;
			}
			if (!minInclusive) {
				lower = Math.nextUp(lower);
			}

			Float upper = (Float) upperNumber;
			if (upper == null) {
				upper = Float.POSITIVE_INFINITY;
			}
			if (!maxInclusive) {
				upper = Math.nextDown(upper);
			}
			return FloatPoint.newRangeQuery(field, lower, upper);
		}
		else if (FieldTypeUtil.isNumericDoubleFieldType(fieldType)) {
			Double lower = (Double) lowerNumber;
			if (lower == null) {
				lower = Double.NEGATIVE_INFINITY;
			}
			if (!minInclusive) {
				lower = Math.nextUp(lower);
			}

			Double upper = (Double) upperNumber;
			if (upper == null) {
				upper = Double.POSITIVE_INFINITY;
			}
			if (!maxInclusive) {
				upper = Math.nextDown(upper);
			}
			return DoublePoint.newRangeQuery(field, lower, upper);
		}
		else {
			throw new QueryNodeException(new MessageImpl(QueryParserMessages.UNSUPPORTED_NUMERIC_DATA_TYPE, fieldType));
		}
	}
}
