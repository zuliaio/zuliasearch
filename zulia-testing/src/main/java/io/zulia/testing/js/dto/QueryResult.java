package io.zulia.testing.js.dto;

import org.graalvm.polyglot.proxy.ProxyObject;

import java.util.List;

public class QueryResult {

	public QueryResult() {

	}

	public long count;

	public List<ProxyObject> doc;

	public FacetProxyObject facet;
}
