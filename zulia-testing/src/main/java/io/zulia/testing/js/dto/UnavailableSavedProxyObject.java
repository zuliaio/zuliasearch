package io.zulia.testing.js.dto;

import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.proxy.ProxyArray;
import org.graalvm.polyglot.proxy.ProxyObject;

import java.util.function.Function;

/**
 * Stands in for {@code saved} when a run has no saved values. Every read throws what {@code onRead} builds for the name, so only the
 * expressions that really reach {@code saved} are affected, including through a function or the script.
 */
public final class UnavailableSavedProxyObject implements ProxyObject {

	private final Function<String, RuntimeException> onRead;

	public UnavailableSavedProxyObject(Function<String, RuntimeException> onRead) {
		this.onRead = onRead;
	}

	@Override
	public Object getMember(String key) {
		throw onRead.apply(key);
	}

	@Override
	public Object getMemberKeys() {
		return ProxyArray.fromArray();
	}

	/**
	 * Claims every name except the ones JS probes by itself, the same way {@link SavedValuesProxyObject} does.
	 */
	@Override
	public boolean hasMember(String key) {
		return !SavedValuesProxyObject.JS_BUILTIN_LOOKUPS.contains(key);
	}

	@Override
	public void putMember(String key, Value value) {
		throw new UnsupportedOperationException("Saved values are read-only, cannot set <" + key + ">");
	}
}
