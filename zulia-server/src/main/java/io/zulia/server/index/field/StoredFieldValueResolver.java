package io.zulia.server.index.field;

import io.zulia.message.ZuliaIndex;
import io.zulia.message.ZuliaIndex.FieldConfig;
import io.zulia.message.ZuliaIndex.FieldConfig.FieldType;
import io.zulia.message.ZuliaIndex.FieldConfig.MalformedValueHandling;
import io.zulia.server.field.FieldDefault;
import io.zulia.server.field.FieldTypeUtil;
import io.zulia.util.BooleanUtil;
import io.zulia.util.LatLon;
import io.zulia.util.ZuliaDateUtil;
import io.zulia.util.ZuliaUtil;
import io.zulia.util.document.DocumentHelper;
import io.zulia.util.document.DocumentHelper.ListElements;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;

/**
 * Resolves a field config's stored value once per document into the handler the facet, sort and index paths read it
 * through, so every path sees the same typed values and the same marker decisions.
 */
public final class StoredFieldValueResolver {

	private static final String GEO_JSON_TYPE = "type";
	private static final String GEO_JSON_POINT = "Point";

	private StoredFieldValueResolver() {
	}

	/**
	 * @param fieldDefault the field's resolved default, or null when it has none
	 */
	public static StoredFieldHandler resolve(Document mongoDocument, FieldConfig fc, FieldDefault fieldDefault) {

		boolean fillEachElement = fieldDefault != null && fieldDefault.fillsEachElement();

		Object o = lookup(mongoDocument, fc, fillEachElement);
		if (o == null) {
			// a defaulted whole value does not count as present
			return fieldDefault != null ? new SingleValuedStoredFieldHandler(fieldDefault.value(), false, false, true) : MissingStoredFieldHandler.INSTANCE;
		}

		// nothing can be substituted and a parse failure throws, so a single stored value needs neither growable list.
		// The markers cannot differ from the loop: no substitution means present, no lenient handling means not malformed
		if (fieldDefault == null && MalformedValueHandling.FAIL.equals(fc.getMalformedValueHandling()) && !(o instanceof Collection<?>)
				&& !(o instanceof Object[])) {
			Object typed = parse(fc, o);
			return new SingleValuedStoredFieldHandler(typed != null ? typed : o, true, false, false);
		}

		List<Object> elements = elements(fc.getFieldType(), o, fillEachElement);
		List<Object> values = new ArrayList<>(elements.size());
		boolean anyReal = false;
		boolean anyLenient = false;
		boolean anyDefaulted = false;

		boolean malformed = false;
		for (Object element : elements) {
			if (element == null) {
				// a null placeholder is only asked for under EACH_ELEMENT, and that mode requires a default
				if (fillEachElement) {
					values.add(fieldDefault.value());
					anyLenient = true;
					anyDefaulted = true;
				}
				continue;
			}

			Object typed;
			try {
				typed = parse(fc, element);
			}
			catch (IllegalArgumentException e) {
				switch (fc.getMalformedValueHandling()) {
					case SKIP -> anyLenient = true;
					case USE_DEFAULT -> {
						if (fieldDefault == null) {
							// the default never passed validation and was dropped, and logged, when the settings loaded, so the
							// value is skipped rather than failing every document that carries one
							anyLenient = true;
						}
						else {
							values.add(fieldDefault.value());
							anyLenient = true;
							anyDefaulted = true;
						}
					}
					case FAIL, UNRECOGNIZED -> throw e;
				}
				malformed = true;
				continue;
			}

			if (typed != null) {
				values.add(typed);
				anyReal = true;
			}
			else {
				// the type reads this value as absent (a blank date string, a geo document with no coordinates). It is not
				// a real value, but it keeps its place so the list length still counts what the document held
				values.add(element);
			}
		}

		// present unless lenient handling had to act and nothing real survived, so an empty list still counts as before
		boolean existsMarker = anyReal || !anyLenient;
		if (values.size() == 1) {
			return new SingleValuedStoredFieldHandler(values.getFirst(), existsMarker, malformed, anyDefaulted);
		}
		return new MultiValuedStoredFieldHandler(values, existsMarker, malformed, anyDefaulted);
	}

	/**
	 * A GEO_POINT with an empty stored field name reads its coordinates from top-level keys, so the document itself is
	 * the point and it is missing when it carries neither key.
	 */
	private static Object lookup(Document mongoDocument, FieldConfig fc, boolean retainNulls) {
		String storedFieldName = fc.getStoredFieldName();
		if (FieldTypeUtil.isGeoPointFieldType(fc.getFieldType()) && storedFieldName.isEmpty()) {
			GeoKeys keys = GeoKeys.of(fc);
			// the document itself is the point, carrying either the configured keys or a GeoJSON Point
			boolean carriesAPoint = mongoDocument.containsKey(keys.latitude()) || mongoDocument.containsKey(keys.longitude()) || GEO_JSON_POINT.equals(
					mongoDocument.get(GEO_JSON_TYPE));
			return carriesAPoint ? mongoDocument : null;
		}
		return DocumentHelper.getValueFromMongoDocument(mongoDocument, storedFieldName, retainNulls ? ListElements.RETAIN_NULL : ListElements.DROP_NULL);
	}

	/**
	 * A vector is one value whose numbers are not elements. Every other type flattens nested lists as before.
	 */
	private static List<Object> elements(FieldType fieldType, Object o, boolean retainNulls) {
		if (FieldTypeUtil.isVectorFieldType(fieldType)) {
			return List.of(o);
		}
		List<Object> elements = new ArrayList<>(o instanceof Collection<?> collection ? collection.size() : 1);
		ZuliaUtil.handleLists(o, elements::add, retainNulls);
		return elements;
	}

	/**
	 * Typed value of one element, or null for a value the type treats as absent (a blank date string, a geo point
	 * document carrying no coordinates). Numbers stay as supplied so the consumers narrow them as before.
	 */
	private static Object parse(FieldConfig fc, Object element) {
		String storedFieldName = fc.getStoredFieldName();
		FieldType fieldType = fc.getFieldType();
		return switch (fieldType) {
			case STRING -> element.toString();
			case NUMERIC_INT -> parseNumber(element, storedFieldName, fieldType, Integer::parseInt);
			case NUMERIC_LONG -> parseNumber(element, storedFieldName, fieldType, Long::parseLong);
			case NUMERIC_FLOAT -> parseNumber(element, storedFieldName, fieldType, Float::parseFloat);
			case NUMERIC_DOUBLE -> parseNumber(element, storedFieldName, fieldType, Double::parseDouble);
			case DATE -> ZuliaDateUtil.convertToDateForField(element, storedFieldName);
			case BOOL -> parseBoolean(element, storedFieldName);
			case UNIT_VECTOR, VECTOR -> parseVector(element, fc);
			case GEO_POINT -> parseGeoPoint(element, fc);
			case UNRECOGNIZED -> throw new IllegalStateException("Unrecognized field type for field <" + storedFieldName + ">");
		};
	}

	private static float[] parseVector(Object element, FieldConfig fc) {
		String storedFieldName = fc.getStoredFieldName();
		if (!(element instanceof Collection<?> numbers)) {
			throw new IllegalArgumentException(
					"Expecting a list of numbers for vector field <" + storedFieldName + "> and found <" + element.getClass().getSimpleName() + ">");
		}
		float[] vector = new float[numbers.size()];
		int i = 0;
		for (Object number : numbers) {
			if (!(number instanceof Number n)) {
				String found = number == null ? "null" : number.getClass().getSimpleName();
				throw new IllegalArgumentException("Vector field <" + storedFieldName + "> element " + i + " is <" + found + "> instead of a number");
			}
			vector[i++] = n.floatValue();
		}
		int dimensions = fc.getVectorDescription().getDimensions();
		if (dimensions > 0 && vector.length != dimensions) {
			throw new IllegalArgumentException(
					"Vector field <" + storedFieldName + "> is configured for " + dimensions + " dimensions but the document has " + vector.length);
		}
		return vector;
	}

	/**
	 * A point is a Document in GeoJSON form ({"type": "Point", "coordinates": [longitude, latitude]}) or one carrying the
	 * configured latitude and longitude keys. A Document with neither is absent, so a document without coordinates under
	 * top-level keys is simply missing. Anything else, including one coordinate without the other, is malformed.
	 */
	private static LatLon parseGeoPoint(Object element, FieldConfig fc) {
		String field = fc.getStoredFieldName().isEmpty() ? "top-level geo point" : "geo point field <" + fc.getStoredFieldName() + ">";
		if (!(element instanceof Document point)) {
			throw new IllegalArgumentException(
					"Expecting a Document with latitude and longitude or a GeoJSON Point for " + field + " and found <" + element.getClass().getSimpleName()
							+ ">");
		}
		try {
			if (GEO_JSON_POINT.equals(point.get(GEO_JSON_TYPE))) {
				Object coordinates = point.get("coordinates");
				if (coordinates instanceof List<?> coords && coords.size() >= 2 && coords.get(0) instanceof Number lon && coords.get(1) instanceof Number lat) {
					return new LatLon(lat.doubleValue(), lon.doubleValue());
				}
				throw new IllegalArgumentException("GeoJSON Point coordinates must be [longitude, latitude] numbers and found <" + coordinates + ">");
			}
			GeoKeys keys = GeoKeys.of(fc);
			Object latitude = point.get(keys.latitude());
			Object longitude = point.get(keys.longitude());
			if (latitude == null && longitude == null) {
				return null;
			}
			if (latitude instanceof Number lat && longitude instanceof Number lon) {
				return new LatLon(lat.doubleValue(), lon.doubleValue());
			}
			throw new IllegalArgumentException(
					"<" + keys.latitude() + "> and <" + keys.longitude() + "> must both be numbers and found <" + latitude + "> and <" + longitude + ">");
		}
		catch (IllegalArgumentException e) {
			throw new IllegalArgumentException(e.getMessage() + " for " + field, e);
		}
	}

	private record GeoKeys(String latitude, String longitude) {

		static GeoKeys of(FieldConfig fc) {
			ZuliaIndex.GeoPointConfig config = fc.getGeoPointConfig();
			return new GeoKeys(config.getLatitudeKey().isEmpty() ? "latitude" : config.getLatitudeKey(),
					config.getLongitudeKey().isEmpty() ? "longitude" : config.getLongitudeKey());
		}
	}

	private static Number parseNumber(Object element, String storedFieldName, FieldType fieldType, Function<String, Number> parser) {
		return switch (element) {
			case Number number -> number;
			case String string -> {
				try {
					yield parser.apply(string);
				}
				catch (NumberFormatException e) {
					throw new IllegalArgumentException("String value <" + string + "> for field <" + storedFieldName + "> cannot be parsed as " + fieldType);
				}
			}
			default -> throw new IllegalArgumentException(
					"Expecting Number or numeric String for field <" + storedFieldName + "> and found <" + element.getClass().getSimpleName() + ">");
		};
	}

	private static Boolean parseBoolean(Object element, String storedFieldName) {
		Boolean boolVal = BooleanUtil.parseBoolean(element);
		if (boolVal == null) {
			throw new IllegalArgumentException(switch (element) {
				case String s -> "String for Boolean field must be 'Yes', 'No', 'Y', 'N', '1', '0', 'True', 'False', 'T', 'F' (case insensitive) for field <"
						+ storedFieldName + "> and found <" + s + ">";
				case Number number -> "Number for Boolean field must be 0 or 1 for field <" + storedFieldName + "> and found <" + number + ">";
				default -> "Expecting Boolean, String, or Number for field <" + storedFieldName + "> and found <" + element.getClass().getSimpleName() + ">";
			});
		}
		return boolVal;
	}
}
