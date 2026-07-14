package io.zulia.server.index.resident;

@FunctionalInterface
public interface LeaseFunction {
	IndexLease lease(String indexName) throws Exception;
}