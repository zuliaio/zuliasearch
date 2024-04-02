package io.zulia.testing.js.dto;

import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.proxy.ProxyArray;
import org.graalvm.polyglot.proxy.ProxyObject;

import java.util.List;
import java.util.Map;

public class FacetProxyObject implements ProxyObject {
	private final Map<String, List<FacetValue>> keyToFacets;

	public FacetProxyObject(Map<String, List<FacetValue>> keyToFacets) {
		this.keyToFacets = keyToFacets;
	}

	@Override
	public void putMember(String key, Value value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean hasMember(String key) {
		return keyToFacets.containsKey(key);
	}

	@Override
	public Object getMemberKeys() {
		return new ProxyArray() {
			private final Object[] keys = keyToFacets.keySet().toArray();

			public void set(long index, Value value) {
				throw new UnsupportedOperationException();
			}

			public long getSize() {
				return keys.length;
			}

			public Object get(long index) {
				if (index < 0 || index > Integer.MAX_VALUE) {
					throw new ArrayIndexOutOfBoundsException();
				}
				return keys[(int) index];
			}
		};
	}

	@Override
	public List<FacetValue> getMember(String key) {
		return keyToFacets.get(key);
	}

}