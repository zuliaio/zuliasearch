package io.zulia.server.field;

import com.google.common.base.Splitter;
import io.zulia.ZuliaFieldConstants;
import io.zulia.message.ZuliaIndex;
import io.zulia.message.ZuliaIndex.FieldConfig;
import io.zulia.server.config.ServerIndexConfig;

import java.util.Arrays;
import java.util.List;

public class FieldTypeUtil {
	private static final Splitter COMMA_SPLIT = Splitter.on(",").omitEmptyStrings();

	public static boolean isNumericIntFieldType(FieldConfig.FieldType fieldType) {
		return FieldConfig.FieldType.NUMERIC_INT.equals(fieldType);
	}

	public static boolean isStoredAsInt(FieldConfig.FieldType fieldType) {
		return isNumericIntFieldType(fieldType) || isBooleanFieldType(fieldType);
	}

	public static boolean isNumericLongFieldType(FieldConfig.FieldType fieldType) {
		return FieldConfig.FieldType.NUMERIC_LONG.equals(fieldType);
	}

	public static boolean isStoredAsLong(FieldConfig.FieldType fieldType) {
		return isNumericLongFieldType(fieldType) || isDateFieldType(fieldType);
	}

	public static boolean isNumericFloatFieldType(FieldConfig.FieldType fieldType) {
		return FieldConfig.FieldType.NUMERIC_FLOAT.equals(fieldType);
	}

	public static boolean isNumericDoubleFieldType(FieldConfig.FieldType fieldType) {
		return FieldConfig.FieldType.NUMERIC_DOUBLE.equals(fieldType);
	}

	public static boolean isDateFieldType(FieldConfig.FieldType fieldType) {
		return FieldConfig.FieldType.DATE.equals(fieldType);
	}

	public static boolean isHandledAsNumericFieldType(FieldConfig.FieldType fieldType) {
		return isNumericFieldType(fieldType) || isDateFieldType(fieldType) || isBooleanFieldType(fieldType);
	}

	public static boolean isNumericFieldType(FieldConfig.FieldType fieldType) {
		return isNumericIntFieldType(fieldType) || isNumericLongFieldType(fieldType) || isNumericFloatFieldType(fieldType) || isNumericDoubleFieldType(
				fieldType);
	}

	public static boolean isNumericFloatingPointFieldType(FieldConfig.FieldType fieldType) {
		return isNumericFloatFieldType(fieldType) || isNumericDoubleFieldType(fieldType);
	}

	public static boolean isBooleanFieldType(FieldConfig.FieldType fieldType) {
		return FieldConfig.FieldType.BOOL.equals(fieldType);
	}

	public static boolean isStringFieldType(FieldConfig.FieldType fieldType) {
		return ZuliaIndex.FieldConfig.FieldType.STRING.equals(fieldType);
	}

	public static boolean isVectorFieldType(FieldConfig.FieldType fieldType) {
		return FieldConfig.FieldType.VECTOR.equals(fieldType) || FieldConfig.FieldType.UNIT_VECTOR.equals(fieldType);
	}

	public static String getListLengthIndexField(String indexFieldName) {
		return ZuliaFieldConstants.LIST_LENGTH_PREFIX + indexFieldName;
	}

	public static String getListLengthSortField(String indexFieldName) {
		return ZuliaFieldConstants.LIST_LENGTH_PREFIX + indexFieldName + ZuliaFieldConstants.SORT_SUFFIX;
	}

	public static boolean isListLengthField(String field) {
		return field.startsWith(ZuliaFieldConstants.LIST_LENGTH_PREFIX);
	}

	public static String getCharLengthIndexField(String indexFieldName) {
		return ZuliaFieldConstants.CHAR_LENGTH_PREFIX + indexFieldName;
	}

	public static String getCharLengthSortField(String indexFieldName) {
		return ZuliaFieldConstants.CHAR_LENGTH_PREFIX + indexFieldName + ZuliaFieldConstants.SORT_SUFFIX;
	}

	public static boolean isCharLengthField(String field) {
		return field.startsWith(ZuliaFieldConstants.CHAR_LENGTH_PREFIX);
	}

	public static String getSortField(String sortFieldName, FieldConfig.FieldType fieldType) {
		return sortFieldName + ZuliaFieldConstants.SORT_SUFFIX + fieldType;
	}

	public static String getIndexField(String indexFieldName, FieldConfig.FieldType fieldType) {
		if (isStringFieldType(fieldType)) {
			return indexFieldName;
		}

		return indexFieldName + "_" + fieldType;
	}

	public static String getCharLengthWrap(String field) {
		return ZuliaFieldConstants.CHAR_LENGTH_BAR + field + ZuliaFieldConstants.CHAR_LENGTH_BAR;
	}

	public static String getListLengthWrap(String field) {
		return ZuliaFieldConstants.LIST_LENGTH_BARS + field + ZuliaFieldConstants.LIST_LENGTH_BARS;
	}

	public static List<String> expandFields(ServerIndexConfig serverIndexConfig, CharSequence... fields) {
		return Arrays.stream(fields).flatMap(COMMA_SPLIT::splitToStream).flatMap(field -> serverIndexConfig.getMatchingFields(field).stream()).toList();
	}
}
