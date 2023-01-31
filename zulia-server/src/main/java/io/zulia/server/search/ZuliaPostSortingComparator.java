package io.zulia.server.search;

import io.zulia.ZuliaConstants;
import io.zulia.message.ZuliaIndex.FieldConfig;
import io.zulia.message.ZuliaQuery;
import io.zulia.message.ZuliaQuery.FieldSort;
import io.zulia.message.ZuliaQuery.ScoredResult;
import io.zulia.message.ZuliaQuery.SortValues;
import io.zulia.server.field.FieldTypeUtil;
import io.zulia.server.search.queryparser.ZuliaParser;
import org.apache.lucene.util.BytesRef;

import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class ZuliaPostSortingComparator implements Comparator<ScoredResult> {

    private final static Comparator<ScoredResult> scoreCompare = new ScoreCompare();
    private final static Comparator<ScoredResult> reverseScoreCompare = new ReverseScoreCompare();

    private final List<FieldSort> fieldSortList;
    private final Map<String, FieldConfig.FieldType> sortTypeMap;

    public ZuliaPostSortingComparator(List<FieldSort> fieldSortList, Map<String, FieldConfig.FieldType> sortTypeMap) {
        this.fieldSortList = fieldSortList;
        this.sortTypeMap = sortTypeMap;
    }

    @Override
    public int compare(ScoredResult o1, ScoredResult o2) {

        if (fieldSortList == null || fieldSortList.isEmpty()) {
            return scoreCompare.compare(o1, o2);
        }

        int compare = 0;

        int sortValueIndex = 0;

        SortValues sortValues1 = o1.getSortValues();
        SortValues sortValues2 = o2.getSortValues();
        for (FieldSort fs : fieldSortList) {
            String sortField = fs.getSortField();

            FieldConfig.FieldType sortType = sortTypeMap.get(sortField);

            if (!ZuliaParser.rewriteLengthFields(sortField).equals(sortField)) {
                sortType = FieldConfig.FieldType.NUMERIC_LONG;
            }

            if (ZuliaConstants.SCORE_FIELD.equals(sortField)) {
                if (FieldSort.Direction.DESCENDING.equals(fs.getDirection())) {
                    compare = scoreCompare.compare(o1, o2);
                } else {
                    compare = reverseScoreCompare.compare(o1, o2);
                }
            } else {
                ZuliaQuery.SortValue sortValue1 = sortValues1.getSortValue(sortValueIndex);
                ZuliaQuery.SortValue sortValue2 = sortValues2.getSortValue(sortValueIndex);

                if (FieldTypeUtil.isNumericIntFieldType(sortType)) {
                    Integer a = sortValue1.getExists() ? sortValue1.getIntegerValue() : null;
                    Integer b = sortValue2.getExists() ? sortValue2.getIntegerValue() : null;

                    if (!fs.getMissingLast()) {
                        compare = Comparator.nullsFirst(Integer::compareTo).compare(a, b);
                    } else {
                        compare = Comparator.nullsLast(Integer::compareTo).compare(a, b);
                    }
                } else if (FieldTypeUtil.isNumericLongFieldType(sortType)) {
                    Long a = sortValue1.getExists() ? sortValue1.getLongValue() : null;
                    Long b = sortValue2.getExists() ? sortValue2.getLongValue() : null;

                    if (!fs.getMissingLast()) {
                        compare = Comparator.nullsFirst(Long::compareTo).compare(a, b);
                    } else {
                        compare = Comparator.nullsLast(Long::compareTo).compare(a, b);
                    }
                } else if (FieldTypeUtil.isDateFieldType(sortType)) {
                    Long a = sortValue1.getExists() ? sortValue1.getDateValue() : null;
                    Long b = sortValue2.getExists() ? sortValue2.getDateValue() : null;

                    if (!fs.getMissingLast()) {
                        compare = Comparator.nullsFirst(Long::compareTo).compare(a, b);
                    } else {
                        compare = Comparator.nullsLast(Long::compareTo).compare(a, b);
                    }
                } else if (FieldTypeUtil.isNumericFloatFieldType(sortType)) {

                    Float a = sortValue1.getExists() ? sortValue1.getFloatValue() : null;
                    Float b = sortValue2.getExists() ? sortValue2.getFloatValue() : null;

                    if (!fs.getMissingLast()) {
                        compare = Comparator.nullsFirst(Float::compareTo).compare(a, b);
                    } else {
                        compare = Comparator.nullsLast(Float::compareTo).compare(a, b);
                    }
                } else if (FieldTypeUtil.isNumericDoubleFieldType(sortType)) {

                    Double a = sortValue1.getExists() ? sortValue1.getDoubleValue() : null;
                    Double b = sortValue2.getExists() ? sortValue2.getDoubleValue() : null;

                    if (!fs.getMissingLast()) {
                        compare = Comparator.nullsFirst(Double::compareTo).compare(a, b);
                    } else {
                        compare = Comparator.nullsLast(Double::compareTo).compare(a, b);
                    }

                } else {
                    String a = sortValue1.getExists() ? sortValue1.getStringValue() : null;
                    String b = sortValue2.getExists() ? sortValue2.getStringValue() : null;

                    if (!fs.getMissingLast()) {
                        compare = Comparator.nullsFirst(BytesRef::compareTo).compare(a != null ? new BytesRef(a) : null, b != null ? new BytesRef(b) : null);
                    } else {
                        compare = Comparator.nullsLast(BytesRef::compareTo).compare(a != null ? new BytesRef(a) : null, b != null ? new BytesRef(b) : null);
                    }
                }

                if (FieldSort.Direction.DESCENDING.equals(fs.getDirection())) {
                    compare *= -1;
                }
            }

            if (compare != 0) {
                return compare;
            }

            sortValueIndex++;

        }

        return compare;
    }

    ;

}
