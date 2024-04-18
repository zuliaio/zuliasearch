package io.zulia.testing.js.dto;

import java.util.List;
import java.util.Map;

public class KeyedListProxyObject<T> extends KeyedProxyObject<List<T>> {

	public KeyedListProxyObject(Map<String, List<T>> keyToFacets) {
		super(keyToFacets);
	}

}