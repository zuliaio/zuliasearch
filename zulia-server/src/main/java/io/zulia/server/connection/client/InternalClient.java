package io.zulia.server.connection.client;

import io.zulia.message.ZuliaBase.Node;
import io.zulia.message.ZuliaServiceOuterClass.*;
import io.zulia.server.connection.client.handler.InternalBatchFetchHandler;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class InternalClient {
	private final static Logger LOG = LoggerFactory.getLogger(InternalClient.class);

	private static final int CONNECTIONS_PER_NODE = 3;

	private final ConcurrentHashMap<String, InternalRpcConnection[]> internalConnectionMap;
	private final AtomicInteger roundRobin;
	private final InternalQueryHandler internalQueryHandler;
	private final InternalStoreHandler internalStoreHandler;
	private final InternalDeleteHandler internalDeleteHandler;
	private final InternalFetchHandler internalFetchHandler;
	private final InternalBatchFetchHandler internalBatchFetchHandler;
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

		this.internalConnectionMap = new ConcurrentHashMap<>();
		this.roundRobin = new AtomicInteger();

		internalQueryHandler = new InternalQueryHandler(this);
		internalStoreHandler = new InternalStoreHandler(this);
		internalDeleteHandler = new InternalDeleteHandler(this);
		internalFetchHandler = new InternalFetchHandler(this);
		internalBatchFetchHandler = new InternalBatchFetchHandler(this);
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
		for (InternalRpcConnection[] connections : internalConnectionMap.values()) {
			for (InternalRpcConnection connection : connections) {
				connection.close();
			}
		}
	}

	public void addNode(Node node) {
		String nodeKey = getNodeKey(node);

		if (!internalConnectionMap.containsKey(nodeKey)) {

			LOG.info("Adding {} connections for node {}", CONNECTIONS_PER_NODE, nodeKey);

			InternalRpcConnection[] connections = new InternalRpcConnection[CONNECTIONS_PER_NODE];
			for (int i = 0; i < CONNECTIONS_PER_NODE; i++) {
				connections[i] = new InternalRpcConnection(node.getServerAddress(), node.getServicePort());
			}

			internalConnectionMap.putIfAbsent(nodeKey, connections);
		}
		else {
			LOG.info("Already loaded connections for node {}", nodeKey);
		}
	}

	public void removeNode(Node node) {
		String nodeKey = getNodeKey(node);

		LOG.info("Removing connections for node {}", nodeKey);
		InternalRpcConnection[] connections = internalConnectionMap.remove(nodeKey);

		if (connections != null) {
			for (InternalRpcConnection connection : connections) {
				connection.close();
			}
		}
		else {
			LOG.info("Already closed connections for node {}", nodeKey);
		}

	}

	private String getNodeKey(Node node) {
		return node.getServerAddress() + ":" + node.getServicePort();
	}

	public InternalRpcConnection getConnection(Node node) throws Exception {
		String nodeKey = getNodeKey(node);
		InternalRpcConnection[] connections = internalConnectionMap.get(nodeKey);
		if (connections != null) {
			int index = Math.floorMod(roundRobin.getAndIncrement(), connections.length);
			return connections[index];
		}
		throw new Exception("Cannot get connection: Node <" + nodeKey + "> not loaded");
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

	public BatchFetchResponse executeBatchFetch(Node node, InternalBatchFetchRequest request) throws Exception {
		return internalBatchFetchHandler.handleRequest(node, request);
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
