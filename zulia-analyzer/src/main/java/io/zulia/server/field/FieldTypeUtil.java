package io.zulia.server.field;

import io.zulia.message.ZuliaIndex;
import io.zulia.message.ZuliaIndex.FieldConfig;

public class FieldTypeUtil {

    public static boolean isNumericIntFieldType(FieldConfig.FieldType fieldType) {
        return FieldConfig.FieldType.NUMERIC_INT.equals(fieldType);
    }

    public static boolean isNumericLongFieldType(FieldConfig.FieldType fieldType) {
        return FieldConfig.FieldType.NUMERIC_LONG.equals(fieldType);
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

    public static boolean isNumericOrDateFieldType(FieldConfig.FieldType fieldType) {
        return isNumericFieldType(fieldType) || isDateFieldType(fieldType);
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
}
