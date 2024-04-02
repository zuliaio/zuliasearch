package io.zulia.testing.js.dto;

import org.bson.Document;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.proxy.ProxyArray;
import org.graalvm.polyglot.proxy.ProxyObject;

public class DocumentProxyObject implements ProxyObject {
	private final Document document;

	public DocumentProxyObject(Document document) {
		this.document = document;
	}

	@Override
	public void putMember(String key, Value value) {
		document.put(key, value.isHostObject() ? value.asHostObject() : value);
	}

	@Override
	public boolean hasMember(String key) {
		return document.containsKey(key);
	}

	@Override
	public Object getMemberKeys() {
		return new ProxyArray() {
			private final Object[] keys = document.keySet().toArray();

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
	public Object getMember(String key) {
		Object v = document.get(key);
		if (v instanceof Document d) {
			return new DocumentProxyObject(d);
		}
		else {
			return v;
		}
	}

	@Override
	public boolean removeMember(String key) {
		if (document.containsKey(key)) {
			document.remove(key);
			return true;
		}
		else {
			return false;
		}
	}

}