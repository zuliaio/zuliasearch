package io.zulia.server.search.queryparser;

import io.zulia.message.ZuliaIndex;
import io.zulia.server.config.IndexFieldInfo;
import io.zulia.server.field.FieldTypeUtil;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class SetQueryHelper {

	public static Query getNumericSetQuery(String field, IndexFieldInfo indexFieldInfo, Supplier<List<Integer>> intSupplier, Supplier<List<Long>> longSupplier,
			Supplier<List<Float>> floatSupplier, Supplier<List<Double>> doubleSupplier) {
		ZuliaIndex.FieldConfig.FieldType fieldType = indexFieldInfo.getFieldType();
		String searchField = indexFieldInfo.getInternalFieldName();
		String sortField = indexFieldInfo.getInternalSortFieldName();

		if (fieldType == null) {
			throw new IllegalArgumentException("Field " + field + " is not indexed");
		}
		else {
			if (FieldTypeUtil.isNumericIntFieldType(fieldType)) {
				List<Integer> integerValueList = intSupplier.get();
				if (integerValueList.isEmpty()) {
					throw new IllegalArgumentException("No integer values for integer field " + field + " for numeric set query");
				}

				Query pointQuery = IntPoint.newSetQuery(searchField, integerValueList);
				if (sortField == null) {
					return pointQuery;
				}
				long[] pointsArray = integerValueList.stream().mapToLong(Integer::intValue).toArray();
				return new IndexOrDocValuesQuery(pointQuery, SortedNumericDocValuesField.newSlowSetQuery(sortField, pointsArray));
			}
			else if (FieldTypeUtil.isNumericLongFieldType(fieldType)) {
				List<Long> longValueList = longSupplier.get();
				if (longValueList.isEmpty()) {
					throw new IllegalArgumentException("No long values for long field " + field + " for numeric set query");
				}

				Query pointQuery = LongPoint.newSetQuery(searchField, longValueList);
				if (sortField == null) {
					return pointQuery;
				}
				long[] pointsArray = longValueList.stream().mapToLong(Long::longValue).toArray();
				return new IndexOrDocValuesQuery(pointQuery, SortedNumericDocValuesField.newSlowSetQuery(sortField, pointsArray));
			}
			else if (FieldTypeUtil.isNumericFloatFieldType(fieldType)) {
				List<Float> floatValueList = floatSupplier.get();
				if (floatValueList.isEmpty()) {
					throw new IllegalArgumentException("No float values for float field <" + field + "> for numeric set query");
				}

				Query pointQuery = FloatPoint.newSetQuery(searchField, floatValueList);
				if (sortField == null) {
					return pointQuery;
				}
				long[] pointsArray = floatValueList.stream().mapToLong(NumericUtils::floatToSortableInt).toArray();
				return new IndexOrDocValuesQuery(pointQuery, SortedNumericDocValuesField.newSlowSetQuery(sortField, pointsArray));
			}
			else if (FieldTypeUtil.isNumericDoubleFieldType(fieldType)) {
				List<Double> doubleValueList = doubleSupplier.get();
				if (doubleValueList.isEmpty()) {
					throw new IllegalArgumentException("No double values for double field " + field + " for numeric set query");
				}

				Query pointQuery = DoublePoint.newSetQuery(searchField, doubleValueList);
				if (sortField == null) {
					return pointQuery;
				}
				long[] pointsArray = doubleValueList.stream().mapToLong(NumericUtils::doubleToSortableLong).toArray();
				return new IndexOrDocValuesQuery(pointQuery, SortedNumericDocValuesField.newSlowSetQuery(sortField, pointsArray));
			}
		}
		throw new IllegalArgumentException("No field type of " + fieldType + " is not supported for numeric set queries");
	}

	public static Query getTermInSetQuery(List<String> terms, String field, IndexFieldInfo indexFieldInfo) {
		List<BytesRef> termBytesRef = new ArrayList<>();
		for (String term : terms) {
			termBytesRef.add(new BytesRef(term));
		}

		if (FieldTypeUtil.isStringFieldType(indexFieldInfo.getFieldType())) {
			String sortField = indexFieldInfo.getInternalSortFieldName();

			if (sortField != null) {
				Query indexQuery = new TermInSetQuery(field, termBytesRef);
				Query dvQuery = new TermInSetQuery(MultiTermQuery.DOC_VALUES_REWRITE, sortField, termBytesRef);
				return new IndexOrDocValuesQuery(indexQuery, dvQuery);
			}

			return new TermInSetQuery(field, termBytesRef);
		}
		throw new IllegalArgumentException(
				"Field type of " + indexFieldInfo.getFieldType() + " is not supported for term queries.  Only STRING is supported.");
	}
}
