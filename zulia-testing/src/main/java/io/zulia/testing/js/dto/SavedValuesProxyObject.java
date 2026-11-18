package io.zulia.testing.js.dto;

import io.zulia.testing.js.SavedUnavailableException;
import org.graalvm.polyglot.proxy.ProxyArray;
import org.graalvm.polyglot.proxy.ProxyObject;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Read-only JS view of values saved by a previous run. Reading a name that was not saved throws instead of yielding undefined. A name the
 * previous run skipped throws {@link SavedUnavailableException}, so the skip carries over to whatever reads it.
 */
public final class SavedValuesProxyObject implements ProxyObject {

	/**
	 * Names JS itself probes on any object (string coercion, JSON.stringify, promise checks). These stay undefined instead of throwing.
	 */
	static final Set<String> JS_BUILTIN_LOOKUPS = Set.of("toJSON", "toString", "valueOf", "constructor", "then");

	private final Map<String, Object> values;
	private final Set<String> skippedNames;

	public SavedValuesProxyObject(Map<String, Object> values) {
		this(values, Set.of());
	}

	public SavedValuesProxyObject(Map<String, Object> values, Set<String> skippedNames) {
		this.values = new TreeMap<>(values);
		this.skippedNames = skippedNames;
	}

	@Override
	public Object getMember(String key) {
		if (values.containsKey(key)) {
			return JsValues.toJs(values.get(key));
		}
		if (skippedNames.contains(key)) {
			throw new SavedUnavailableException("Uses saved." + key + ", which the previous run skipped and did not record");
		}
		throw new IllegalArgumentException("Saved value <" + key + "> was not found in the saved values. Available saved values: " + values.keySet());
	}

	@Override
	public Object getMemberKeys() {
		return ProxyArray.fromList(List.copyOf(values.keySet()));
	}

	/**
	 * GraalJS asks this before getMember and yields undefined on false, so claim every name and let getMember throw for unknown ones.
	 */
	@Override
	public boolean hasMember(String key) {
		return values.containsKey(key) || !JS_BUILTIN_LOOKUPS.contains(key);
	}

	@Override
	public void putMember(String key, org.graalvm.polyglot.Value value) {
		throw new UnsupportedOperationException("Saved values are read-only, cannot set <" + key + ">");
	}

	public Map<String, Object> values() {
		return values;
	}
}
