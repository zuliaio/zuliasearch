package io.zulia.util.document;

import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Matt Davis on 2/1/16.
 */
public class DocumentHelper {

	/**
	 * How list elements met while resolving a dotted path are treated. Every other value is returned as stored.
	 */
	public enum ListElements {
		/**
		 * drop null elements and elements lacking the sub-field, the plain lookup
		 */
		DROP_NULL,
		/**
		 * keep a null placeholder for each null element and each element lacking the sub-field, and carry that
		 * placeholder through every deeper level of the path, so parallel lists stay aligned
		 */
		RETAIN_NULL,
		/**
		 * RETAIN_NULL, and a nested list met on the way down stays one entry holding its own values instead of being
		 * flattened, so the result has one entry per outer element and lines up with a sibling path one level up
		 */
		RETAIN_NULL_NESTED;

		boolean retainsNull() {
			return this != DROP_NULL;
		}

		boolean keepsNesting() {
			return this == RETAIN_NULL_NESTED;
		}
	}

	public static Object getValueFromMongoDocument(Document mongoDocument, String storedFieldName) {
		return getValueFromMongoDocument(mongoDocument, storedFieldName, ListElements.DROP_NULL);
	}

	/**
	 * @deprecated use {@link #getValueFromMongoDocument(Document, String, ListElements)}, {@code true} is {@link ListElements#RETAIN_NULL}
	 */
	@Deprecated
	public static Object getValueFromMongoDocument(Document mongoDocument, String storedFieldName, boolean retainNull) {
		return getValueFromMongoDocument(mongoDocument, storedFieldName, retainNull ? ListElements.RETAIN_NULL : ListElements.DROP_NULL);
	}

	/**
	 * Resolves a dotted path into the document, treating the list elements met on the way as {@code listElements} says.
	 * Returns null when the path resolves to nothing.
	 */
	public static Object getValueFromMongoDocument(Document mongoDocument, String storedFieldName, ListElements listElements) {

		int next = storedFieldName.indexOf('.');
		if (next >= 0) {
			Object o = mongoDocument;

			int off = 0;
			while (next != -1) {
				String field = storedFieldName.substring(off, next);
				off = next + 1;
				o = getChild(o, field, listElements);
				if (o == null) {
					return null;
				}
				next = storedFieldName.indexOf('.', off);
			}
			String field = storedFieldName.substring(off);
			return getChild(o, field, listElements);
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

	private static Object getChild(Object o, String field, ListElements listElements) {
		if (o instanceof Document d) {
			o = d.get(field);
			if (o instanceof List<?> list) {
				List<Object> values = new ArrayList<>(list.size());
				for (Object item : list) {
					if (item != null || listElements.retainsNull()) {
						values.add(item);
					}
				}
				o = values;
			}
		}
		else if (o instanceof List<?> list) {
			List<Object> values = new ArrayList<>(list.size());
			collectFromListElements(list, field, listElements, values);
			o = values.isEmpty() ? null : values;
		}
		else {
			o = null;
		}
		return o;
	}

	/**
	 * A list met on the way down holds documents to take the field from, nested lists to descend into (a list of
	 * documents that each hold a list of documents), null placeholders to carry through, and scalars to skip. A nested
	 * list is flattened, matching how the index and result paths consume list values, unless the mode keeps nesting, in
	 * which case it becomes one entry holding its own values.
	 */
	private static void collectFromListElements(List<?> list, String field, ListElements listElements, List<Object> values) {
		for (Object item : list) {
			switch (item) {
				case Document d -> {
					Object object = d.get(field);
					if (object != null || listElements.retainsNull()) {
						values.add(object);
					}
				}
				case List<?> nested -> {
					if (listElements.keepsNesting()) {
						List<Object> inner = new ArrayList<>(nested.size());
						collectFromListElements(nested, field, listElements, inner);
						values.add(inner);
					}
					else {
						collectFromListElements(nested, field, listElements, values);
					}
				}
				case null -> {
					if (listElements.retainsNull()) {
						values.add(null);
					}
				}
				default -> {
					// a scalar where sibling elements are sub-documents. It has no sub-field, so under a retaining mode it
					// still holds a place and the result lines up with the list one level up
					if (listElements.retainsNull()) {
						values.add(null);
					}
				}
			}
		}
	}

}
