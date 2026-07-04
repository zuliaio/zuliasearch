package io.zulia.util.document;

import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Matt Davis on 2/1/16.
 */
public class DocumentHelper {

	public static Object getValueFromMongoDocument(Document mongoDocument, String storedFieldName) {
		return getValueFromMongoDocument(mongoDocument, storedFieldName, false);
	}

	public static Object getValueFromMongoDocument(Document mongoDocument, String storedFieldName, boolean retainNullAndEmpty) {

		int next = storedFieldName.indexOf('.');
		if (next >= 0) {
			Object o = mongoDocument;

			int off = 0;
			while (next != -1) {
				String field = storedFieldName.substring(off, next);
				off = next + 1;
				o = getChild(o, field, retainNullAndEmpty);
				if (o == null) {
					return null;
				}
				next = storedFieldName.indexOf('.', off);
			}
			String field = storedFieldName.substring(off);
			return getChild(o, field, retainNullAndEmpty);
		}
		else {
			return mongoDocument.get(storedFieldName);
		}

	}

	public static Float getAsFloat(Document document, String field) {
		return document.get(field) instanceof Number number ? number.floatValue() : null;
	}

	public static Float getAsFloat(Document document, String field, Float defaultValue) {
		Float value = getAsFloat(document, field);
		return value != null ? value : defaultValue;
	}

	public static float getAsFloat(Document document, String field, float defaultValue) {
		Float value = getAsFloat(document, field);
		return value != null ? value : defaultValue;
	}

	public static Double getAsDouble(Document document, String field) {
		return document.get(field) instanceof Number number ? number.doubleValue() : null;
	}

	public static Double getAsDouble(Document document, String field, Double defaultValue) {
		Double value = getAsDouble(document, field);
		return value != null ? value : defaultValue;
	}

	public static double getAsDouble(Document document, String field, double defaultValue) {
		Double value = getAsDouble(document, field);
		return value != null ? value : defaultValue;
	}

	public static Integer getAsInt(Document document, String field) {
		if (document.get(field) instanceof Number number) {
			try {
				return Math.toIntExact(number.longValue());
			}
			catch (ArithmeticException e) {
				throw new ArithmeticException("Field <" + field + "> has value <" + number + "> that does not fit in an int");
			}
		}
		return null;
	}

	public static Integer getAsInt(Document document, String field, Integer defaultValue) {
		Integer value = getAsInt(document, field);
		return value != null ? value : defaultValue;
	}

	public static int getAsInt(Document document, String field, int defaultValue) {
		Integer value = getAsInt(document, field);
		return value != null ? value : defaultValue;
	}

	public static Long getAsLong(Document document, String field) {
		return document.get(field) instanceof Number number ? number.longValue() : null;
	}

	public static Long getAsLong(Document document, String field, Long defaultValue) {
		Long value = getAsLong(document, field);
		return value != null ? value : defaultValue;
	}

	public static long getAsLong(Document document, String field, long defaultValue) {
		Long value = getAsLong(document, field);
		return value != null ? value : defaultValue;
	}

	private static Object getChild(Object o, String field, boolean retainNullAndEmpty) {
		if (o instanceof Document d) {
			o = d.get(field);
			if (!retainNullAndEmpty && o instanceof List<?>) {
				List<Object> values = new ArrayList<>();
				for (Object item : (List<?>) o) {
					if (item != null) {
						if (item instanceof String) {
							if (!((String) item).isEmpty()) {
								values.add(item);
							}
						}
						else {
							values.add(item);
						}
					}
				}
				o = values;
			}
		}
		else if (o instanceof List<?> list) {
			List<Object> values = new ArrayList<>(list.size());
			for (Object item : list) {
				if (item instanceof Document d) {
					Object object = d.get(field);
					if (retainNullAndEmpty) {
						values.add(object);
					}
					else if (object != null) {
						values.add(object);
					}
				}
			}
			if (!values.isEmpty()) {
				o = values;
			}
			else {
				o = null;
			}
		}
		else {
			o = null;
		}
		return o;
	}

}
