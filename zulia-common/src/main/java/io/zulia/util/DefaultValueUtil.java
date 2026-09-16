package io.zulia.util;

import io.zulia.message.ZuliaIndex.DefaultValue;
import io.zulia.message.ZuliaIndex.FieldConfig.FieldType;
import io.zulia.message.ZuliaIndex.FieldConfig.MalformedValueHandling;
import io.zulia.message.ZuliaIndex.GeoPointValue;

import java.util.Date;

public final class DefaultValueUtil {

	private DefaultValueUtil() {
	}

	/**
	 * The oneof case for a field type, or null for the vector types, which take no default.
	 */
	public static DefaultValue.ValueCase expectedCase(FieldType fieldType) {
		return switch (fieldType) {
			case STRING -> DefaultValue.ValueCase.STRINGVALUE;
			case NUMERIC_INT -> DefaultValue.ValueCase.INTVALUE;
			case NUMERIC_LONG -> DefaultValue.ValueCase.LONGVALUE;
			case NUMERIC_FLOAT -> DefaultValue.ValueCase.FLOATVALUE;
			case NUMERIC_DOUBLE -> DefaultValue.ValueCase.DOUBLEVALUE;
			case BOOL -> DefaultValue.ValueCase.BOOLVALUE;
			case DATE -> DefaultValue.ValueCase.DATEVALUE;
			case GEO_POINT -> DefaultValue.ValueCase.GEOPOINTVALUE;
			case UNIT_VECTOR, VECTOR, UNRECOGNIZED -> null;
		};
	}

	public static boolean takesNoDefault(FieldType fieldType) {
		return expectedCase(fieldType) == null;
	}

	public static void validateMalformedValueHandling(String storedFieldName, FieldType fieldType, MalformedValueHandling handling, boolean hasDefaultValue) {
		switch (handling) {
			case FAIL, SKIP -> {
			}
			case USE_DEFAULT -> {
				if (takesNoDefault(fieldType)) {
					throw new IllegalArgumentException("Field <" + storedFieldName + "> is " + fieldType
							+ " which takes no defaultValue, so malformedValueHandling USE_DEFAULT is not available");
				}
				if (!hasDefaultValue) {
					throw new IllegalArgumentException("Field <" + storedFieldName + "> has malformedValueHandling USE_DEFAULT but no defaultValue is set");
				}
			}
			case UNRECOGNIZED -> throw new IllegalArgumentException("Field <" + storedFieldName + "> has an unrecognized malformedValueHandling");
		}
	}

	/**
	 * Rejects a default whose case is not the one case for the field type. There is no widening or narrowing.
	 */
	public static void validate(String storedFieldName, FieldType fieldType, DefaultValue defaultValue) {
		if (FieldType.UNRECOGNIZED.equals(fieldType)) {
			throw new IllegalArgumentException("Field <" + storedFieldName + "> has an unrecognized fieldType, so its defaultValue cannot be checked");
		}
		DefaultValue.ValueCase expected = expectedCase(fieldType);
		if (expected == null) {
			throw new IllegalArgumentException(
					"Field <" + storedFieldName + "> is " + fieldType + " which does not support a defaultValue, found " + describe(defaultValue));
		}
		if (defaultValue.getValueCase() == DefaultValue.ValueCase.VALUE_NOT_SET) {
			throw new IllegalArgumentException(
					"Field <" + storedFieldName + "> is " + fieldType + " and its defaultValue must set " + caseName(expected) + " but no value is set");
		}
		if (defaultValue.getValueCase() != expected) {
			throw new IllegalArgumentException(
					"Field <" + storedFieldName + "> is " + fieldType + " but defaultValue is " + describe(defaultValue) + ", expected " + caseName(expected));
		}
		if (expected == DefaultValue.ValueCase.GEOPOINTVALUE) {
			GeoPointValue point = defaultValue.getGeoPointValue();
			if (!LatLon.isValidLatitude(point.getLatitude())) {
				throw new IllegalArgumentException("Field <" + storedFieldName + "> defaultValue " + LatLon.invalidLatitude(point.getLatitude()));
			}
			if (!LatLon.isValidLongitude(point.getLongitude())) {
				throw new IllegalArgumentException("Field <" + storedFieldName + "> defaultValue " + LatLon.invalidLongitude(point.getLongitude()));
			}
		}
		if (DefaultValue.Fill.UNRECOGNIZED.equals(defaultValue.getFill())) {
			// an unknown fill would otherwise behave as WHOLE_VALUE and silently stop filling list elements
			throw new IllegalArgumentException("Field <" + storedFieldName + "> defaultValue has an unrecognized fill <" + defaultValue.getFillValue() + ">");
		}
	}

	/**
	 * The Java value for the set case, the same type a stored document supplies.
	 */
	public static Object resolve(DefaultValue defaultValue) {
		return switch (defaultValue.getValueCase()) {
			case STRINGVALUE -> defaultValue.getStringValue();
			case INTVALUE -> defaultValue.getIntValue();
			case LONGVALUE -> defaultValue.getLongValue();
			case FLOATVALUE -> defaultValue.getFloatValue();
			case DOUBLEVALUE -> defaultValue.getDoubleValue();
			case BOOLVALUE -> defaultValue.getBoolValue();
			case DATEVALUE -> new Date(defaultValue.getDateValue());
			case GEOPOINTVALUE -> {
				GeoPointValue point = defaultValue.getGeoPointValue();
				yield new LatLon(point.getLatitude(), point.getLongitude());
			}
			case VALUE_NOT_SET -> throw new IllegalArgumentException("defaultValue has no value set");
		};
	}

	/**
	 * Case name and value for error messages, for example {@code stringValue <zero>}.
	 */
	public static String describe(DefaultValue defaultValue) {
		DefaultValue.ValueCase valueCase = defaultValue.getValueCase();
		if (valueCase == DefaultValue.ValueCase.VALUE_NOT_SET) {
			return "no value";
		}
		if (valueCase == DefaultValue.ValueCase.GEOPOINTVALUE) {
			// described without resolving so an out-of-range default can still be named in its own rejection
			GeoPointValue point = defaultValue.getGeoPointValue();
			return caseName(valueCase) + " <" + point.getLatitude() + "," + point.getLongitude() + ">";
		}
		return caseName(valueCase) + " <" + resolve(defaultValue) + ">";
	}

	private static String caseName(DefaultValue.ValueCase valueCase) {
		if (valueCase == DefaultValue.ValueCase.VALUE_NOT_SET) {
			return "no value";
		}
		return DefaultValue.getDescriptor().findFieldByNumber(valueCase.getNumber()).getName();
	}
}
