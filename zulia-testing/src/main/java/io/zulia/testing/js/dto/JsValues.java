package io.zulia.testing.js.dto;

import org.graalvm.polyglot.proxy.ProxyArray;
import org.graalvm.polyglot.proxy.ProxyObject;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Wraps the plain maps and lists that YAML and JSON produce so JavaScript sees ordinary objects and arrays.
 */
public final class JsValues {

	private JsValues() {
	}

	@SuppressWarnings("unchecked")
	public static Object toJs(Object value) {
		return switch (value) {
			case Map<?, ?> map -> ProxyObject.fromMap(((Map<String, Object>) map).entrySet().stream()
					.collect(LinkedHashMap::new, (m, e) -> m.put(e.getKey(), toJs(e.getValue())), LinkedHashMap::putAll));
			case List<?> list -> ProxyArray.fromList(list.stream().map(JsValues::toJs).toList());
			case null, default -> value;
		};
	}
}
