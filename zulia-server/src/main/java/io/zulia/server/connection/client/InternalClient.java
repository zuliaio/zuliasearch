package io.zulia.server.connection.client;

import io.zulia.message.ZuliaBase.Node;
import io.zulia.message.ZuliaServiceOuterClass.*;
import io.zulia.server.connection.client.handler.InternalClearHandler;
import io.zulia.server.connection.client.handler.InternalCreateIndexAliasHandler;
import io.zulia.server.connection.client.handler.InternalCreateOrUpdateIndexHandler;
import io.zulia.server.connection.client.handler.InternalDeleteHandler;
import io.zulia.server.connection.client.handler.InternalDeleteIndexAliasHandler;
import io.zulia.server.connection.client.handler.InternalDeleteIndexHandler;
import io.zulia.server.connection.client.handler.InternalFetchHandler;
import io.zulia.server.connection.client.handler.InternalGetFieldNamesHandler;
import io.zulia.server.connection.client.handler.InternalGetNumberOfDocsHandler;
import io.zulia.server.connection.client.handler.InternalGetTermsHandler;
import io.zulia.server.connection.client.handler.InternalOptimizeHandler;
import io.zulia.server.connection.client.handler.InternalQueryHandler;
import io.zulia.server.connection.client.handler.InternalReindexHandler;
import io.zulia.server.connection.client.handler.InternalStoreHandler;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;

public class InternalClient {
	private final static Logger LOG = LoggerFactory.getLogger(InternalClient.class);
	private final ConcurrentHashMap<String, GenericObjectPool<InternalRpcConnection>> internalConnectionPoolMap;
	private final InternalQueryHandler internalQueryHandler;
	private final InternalStoreHandler internalStoreHandler;
	private final InternalDeleteHandler internalDeleteHandler;
	private final InternalFetchHandler internalFetchHandler;
	private final InternalGetNumberOfDocsHandler internalGetNumberOfDocsHandler;
	private final InternalOptimizeHandler internalOptimizeHandler;
	private final InternalGetFieldNamesHandler internalGetFieldNamesHandler;
	private final InternalClearHandler internalClearHandler;
	private final InternalGetTermsHandler internalGetTermsHandler;
	private final InternalCreateOrUpdateIndexHandler internalCreateIndexHandler;
	private final InternalDeleteIndexHandler internalDeleteIndexHandler;
	private final InternalReindexHandler internalReindexHandler;
	private final InternalCreateIndexAliasHandler internalCreateIndexAliasHandler;
	private final InternalDeleteIndexAliasHandler internalDeleteIndexAliasHandler;

	public InternalClient() {

		this.internalConnectionPoolMap = new ConcurrentHashMap<>();

		internalQueryHandler = new InternalQueryHandler(this);
		internalStoreHandler = new InternalStoreHandler(this);
		internalDeleteHandler = new InternalDeleteHandler(this);
		internalFetchHandler = new InternalFetchHandler(this);
		internalGetNumberOfDocsHandler = new InternalGetNumberOfDocsHandler(this);
		internalOptimizeHandler = new InternalOptimizeHandler(this);
		internalGetFieldNamesHandler = new InternalGetFieldNamesHandler(this);
		internalClearHandler = new InternalClearHandler(this);
		internalGetTermsHandler = new InternalGetTermsHandler(this);
		internalCreateIndexHandler = new InternalCreateOrUpdateIndexHandler(this);
		internalDeleteIndexHandler = new InternalDeleteIndexHandler(this);
		internalReindexHandler = new InternalReindexHandler(this);
		internalCreateIndexAliasHandler = new InternalCreateIndexAliasHandler(this);
		internalDeleteIndexAliasHandler = new InternalDeleteIndexAliasHandler(this);
	}

	public void close() {

		for (GenericObjectPool<InternalRpcConnection> pool : internalConnectionPoolMap.values()) {
			pool.close();
		}
	}

	public void addNode(Node node) {
		String nodeKey = getNodeKey(node);

		if (!internalConnectionPoolMap.containsKey(nodeKey)) {

			LOG.info("Adding connection pool for node {}", nodeKey);

			GenericObjectPool<InternalRpcConnection> pool = new GenericObjectPool<>(
					new InternalRpcConnectionFactory(node.getServerAddress(), node.getServicePort()));
			pool.setMinIdle(4);
			pool.setMaxTotal(64);
			pool.setMinEvictableIdle(Duration.ofMinutes(5));

			internalConnectionPoolMap.putIfAbsent(nodeKey, pool);
		}
		else {
			LOG.info("Already loaded connection for node {}", nodeKey);
		}
	}

	public void removeNode(Node node) {
		String nodeKey = getNodeKey(node);

		LOG.info("Removing connection pool for node {}", nodeKey);
		GenericObjectPool<InternalRpcConnection> connectionPool = internalConnectionPoolMap.remove(nodeKey);

		if (connectionPool != null) {
			connectionPool.close();
		}
		else {
			LOG.info("Already closed connection for node {}", nodeKey);
		}

	}

	private String getNodeKey(Node node) {
		return node.getServerAddress() + ":" + node.getServicePort();
	}

	public InternalRpcConnection getInternalRpcConnection(Node node) throws Exception {
		String nodeKey = getNodeKey(node);
		GenericObjectPool<InternalRpcConnection> connectionPool = internalConnectionPoolMap.get(nodeKey);
		if (connectionPool != null) {
			return connectionPool.borrowObject();
		}
		throw new Exception("Cannot get connection: Node <" + nodeKey + "> not loaded");

	}

	public void returnInternalBlockingConnection(Node node, InternalRpcConnection rpcConnection, boolean valid) {
		String nodeKey = getNodeKey(node);
		GenericObjectPool<InternalRpcConnection> connectionPool = internalConnectionPoolMap.get(nodeKey);
		if (connectionPool != null) {
			try {
				if (valid) {
					connectionPool.returnObject(rpcConnection);
				}
				else {
					connectionPool.invalidateObject(rpcConnection);
				}
			}
			catch (Exception e) {
				LOG.error("Failed to return blocking connection to node {} pool", nodeKey, e);
			}
		}
		else {
			LOG.error("Failed to return blocking connection to node {} pool. Pool does not exist.", nodeKey);
			LOG.error("Current pool members {}", internalConnectionPoolMap.keySet());
			if (rpcConnection != null) {
				rpcConnection.close();
			}
		}
	}

	public InternalQueryResponse executeQuery(Node node, InternalQueryRequest request) throws Exception {
		return internalQueryHandler.handleRequest(node, request);
	}

	public StoreResponse executeStore(Node node, StoreRequest request) throws Exception {
		return internalStoreHandler.handleRequest(node, request);
	}

	public DeleteResponse executeDelete(Node node, DeleteRequest request) throws Exception {
		return internalDeleteHandler.handleRequest(node, request);
	}

	public FetchResponse executeFetch(Node node, FetchRequest request) throws Exception {
		return internalFetchHandler.handleRequest(node, request);
	}

	public GetNumberOfDocsResponse getNumberOfDocs(Node node, InternalGetNumberOfDocsRequest request) throws Exception {
		return internalGetNumberOfDocsHandler.handleRequest(node, request);
	}

	public OptimizeResponse optimize(Node node, OptimizeRequest request) throws Exception {
		return internalOptimizeHandler.handleRequest(node, request);
	}

	public GetFieldNamesResponse getFieldNames(Node node, InternalGetFieldNamesRequest request) throws Exception {
		return internalGetFieldNamesHandler.handleRequest(node, request);
	}

	public ClearResponse clear(Node node, ClearRequest request) throws Exception {
		return internalClearHandler.handleRequest(node, request);
	}

	public InternalGetTermsResponse getTerms(Node node, InternalGetTermsRequest request) throws Exception {
		return internalGetTermsHandler.handleRequest(node, request);
	}

	public InternalCreateOrUpdateIndexResponse createOrUpdateIndex(Node node, InternalCreateOrUpdateIndexRequest request) throws Exception {
		return internalCreateIndexHandler.handleRequest(node, request);
	}

	public DeleteIndexResponse deleteIndex(Node node, DeleteIndexRequest request) throws Exception {
		return internalDeleteIndexHandler.handleRequest(node, request);
	}

	public ReindexResponse reindex(Node node, ReindexRequest request) throws Exception {
		return internalReindexHandler.handleRequest(node, request);
	}

	public CreateIndexAliasResponse createIndexAlias(Node node, InternalCreateIndexAliasRequest request) throws Exception {
		return internalCreateIndexAliasHandler.handleRequest(node, request);
	}

	public DeleteIndexAliasResponse deleteIndexAlias(Node node, DeleteIndexAliasRequest request) throws Exception {
		return internalDeleteIndexAliasHandler.handleRequest(node, request);
	}
}
