package io.zulia.server.search.queryparser.builder;

import io.zulia.message.ZuliaIndex.FieldConfig.FieldType;
import io.zulia.server.config.IndexFieldInfo;
import io.zulia.server.field.FieldTypeUtil;
import io.zulia.server.search.queryparser.node.ZuliaPointRangeQueryNode;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.queryparser.flexible.core.QueryNodeException;
import org.apache.lucene.queryparser.flexible.core.messages.QueryParserMessages;
import org.apache.lucene.queryparser.flexible.core.nodes.QueryNode;
import org.apache.lucene.queryparser.flexible.messages.MessageImpl;
import org.apache.lucene.queryparser.flexible.standard.builders.StandardQueryBuilder;
import org.apache.lucene.queryparser.flexible.standard.nodes.PointQueryNode;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.IndexSortSortedNumericDocValuesRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.NumericUtils;

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

		IndexFieldInfo indexFieldInfo = numericRangeNode.getIndexFieldMapping();
		FieldType fieldType = indexFieldInfo.getFieldType();

		String field = indexFieldInfo.getInternalFieldName();
		String sortField = indexFieldInfo.getInternalSortFieldName();
		boolean minInclusive = numericRangeNode.isLowerInclusive();
		boolean maxInclusive = numericRangeNode.isUpperInclusive();

		if (FieldTypeUtil.isStoredAsInt(fieldType)) {
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
			Query fallbackQuery = new IndexOrDocValuesQuery(IntPoint.newRangeQuery(field, lower, upper),
					SortedNumericDocValuesField.newSlowRangeQuery(sortField, lower, upper));
			return new IndexSortSortedNumericDocValuesRangeQuery(sortField, lower, upper, fallbackQuery);
		}
		else if (FieldTypeUtil.isStoredAsLong(fieldType)) {
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
			Query pointQuery = LongPoint.newRangeQuery(field, lower, upper);
			if (sortField == null) {
				return pointQuery;
			}
			Query fallbackQuery = new IndexOrDocValuesQuery(pointQuery, SortedNumericDocValuesField.newSlowRangeQuery(sortField, lower, upper));
			return new IndexSortSortedNumericDocValuesRangeQuery(sortField, lower, upper, fallbackQuery);
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
			Query pointQuery = FloatPoint.newRangeQuery(field, lower, upper);
			if (sortField == null) {
				return pointQuery;
			}
			return new IndexOrDocValuesQuery(pointQuery,
					SortedNumericDocValuesField.newSlowRangeQuery(sortField, NumericUtils.floatToSortableInt(lower), NumericUtils.floatToSortableInt(upper)));
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
			Query pointQuery = DoublePoint.newRangeQuery(field, lower, upper);
			if (sortField == null) {
				return pointQuery;
			}
			return new IndexOrDocValuesQuery(pointQuery, SortedNumericDocValuesField.newSlowRangeQuery(sortField, NumericUtils.doubleToSortableLong(lower),
					NumericUtils.doubleToSortableLong(upper)));
		}
		else {
			throw new QueryNodeException(new MessageImpl(QueryParserMessages.UNSUPPORTED_NUMERIC_DATA_TYPE, fieldType));
		}
	}
}
