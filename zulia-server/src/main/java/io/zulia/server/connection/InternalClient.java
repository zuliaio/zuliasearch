package io.zulia.server.connection;

import io.zulia.message.ZuliaBase.Node;
import io.zulia.message.ZuliaServiceOuterClass.*;
import io.zulia.server.connection.handler.InternalClearHandler;
import io.zulia.server.connection.handler.InternalDeleteHandler;
import io.zulia.server.connection.handler.InternalFetchHandler;
import io.zulia.server.connection.handler.InternalGetFieldNamesHandler;
import io.zulia.server.connection.handler.InternalGetNumberOfDocsHandler;
import io.zulia.server.connection.handler.InternalGetTermsHandler;
import io.zulia.server.connection.handler.InternalOptimizeHandler;
import io.zulia.server.connection.handler.InternalQueryHandler;
import io.zulia.server.connection.handler.InternalStoreHandler;
import org.apache.commons.pool2.impl.GenericObjectPool;

import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class InternalClient {
	private final static Logger LOG = Logger.getLogger(InternalClient.class.getSimpleName());

	private ConcurrentHashMap<String, GenericObjectPool<InternalRpcConnection>> internalConnectionPoolMap;

	private final InternalQueryHandler internalQueryHandler;
	private final InternalStoreHandler internalStoreHandler;
	private final InternalDeleteHandler internalDeleteHandler;
	private final InternalFetchHandler internalFetchHandler;
	private final InternalGetNumberOfDocsHandler internalGetNumberOfDocsHandler;
	private final InternalOptimizeHandler internalOptimizeHandler;
	private final InternalGetFieldNamesHandler internalGetFieldNamesHandler;
	private final InternalClearHandler internalClearHandler;
	private final InternalGetTermsHandler internalGetTermsHandler;

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
	}

	public void close() {

		for (GenericObjectPool<InternalRpcConnection> pool : internalConnectionPoolMap.values()) {
			try {
				pool.close();
			}
			catch (Exception e) {
				LOG.log(Level.SEVERE, e.getMessage(), e);
			}

		}
	}

	public void addNode(Node node) throws Exception {
		String nodeKey = getNodeKey(node);

		if (!internalConnectionPoolMap.containsKey(nodeKey)) {

			LOG.info("Adding connection pool for node: " + nodeKey);

			GenericObjectPool<InternalRpcConnection> pool = new GenericObjectPool<>(
					new InternalRpcConnectionFactory(node.getServerAddress(), node.getServicePort()));
			pool.setMinIdle(1);
			pool.setMaxTotal(8);
			pool.setMinEvictableIdleTimeMillis(1000L * 60L * 5L);

			internalConnectionPoolMap.putIfAbsent(nodeKey, pool);
		}
		else {
			LOG.info("Already loaded connection for node <" + nodeKey + ">");
		}
	}

	public void removeNode(Node node) {
		String nodeKey = getNodeKey(node);

		LOG.info("Removing connection pool for node <" + nodeKey + ">");
		GenericObjectPool<InternalRpcConnection> connectionPool = internalConnectionPoolMap.remove(nodeKey);

		if (connectionPool != null) {
			connectionPool.close();
		}
		else {
			LOG.info("Already closed connection for node <" + nodeKey + ">");
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
				LOG.log(Level.SEVERE, "Failed to return blocking connection to node <" + nodeKey + "> pool", e);
			}
		}
		else {
			LOG.severe("Failed to return blocking connection to node <" + nodeKey + "> pool. Pool does not exist.");
			LOG.severe("Current pool members <" + internalConnectionPoolMap.keySet() + ">");
			if (rpcConnection != null) {
				rpcConnection.close();
			}
		}
	}

	public InternalQueryResponse executeQuery(Node node, QueryRequest request) throws Exception {
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

	public GetNumberOfDocsResponse getNumberOfDocs(Node node, GetNumberOfDocsRequest request) throws Exception {
		return internalGetNumberOfDocsHandler.handleRequest(node, request);
	}

	public OptimizeResponse optimize(Node node, OptimizeRequest request) throws Exception {
		return internalOptimizeHandler.handleRequest(node, request);
	}

	public GetFieldNamesResponse getFieldNames(Node node, GetFieldNamesRequest request) throws Exception {
		return internalGetFieldNamesHandler.handleRequest(node, request);
	}

	public ClearResponse clear(Node node, ClearRequest request) throws Exception {
		return internalClearHandler.handleRequest(node, request);
	}

	public GetTermsResponseInternal getTerms(Node node, GetTermsRequest request) throws Exception {
		return internalGetTermsHandler.handleRequest(node, request);
	}

}
