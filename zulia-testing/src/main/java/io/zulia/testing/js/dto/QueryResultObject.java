package io.zulia.testing.js.dto;

import org.graalvm.polyglot.proxy.ProxyObject;

import java.util.List;

public class QueryResultObject {

	public QueryResultObject() {

	}

	public long count;

	public List<ProxyObject> doc;

	public KeyedListProxyObject<FacetValueObject> facet;

	public KeyedListProxyObject<StatFacetValueObject> statFacet;

}
