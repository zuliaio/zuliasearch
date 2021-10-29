package io.zulia.server.index;

import io.zulia.message.ZuliaBase;
import io.zulia.message.ZuliaBase.MasterSlaveSettings;
import io.zulia.message.ZuliaBase.Node;
import io.zulia.message.ZuliaIndex.IndexMapping;
import io.zulia.message.ZuliaIndex.IndexSettings;
import io.zulia.message.ZuliaIndex.ShardMapping;
import io.zulia.message.ZuliaQuery;
import io.zulia.message.ZuliaServiceOuterClass.*;
import io.zulia.server.config.IndexService;
import io.zulia.server.config.NodeService;
import io.zulia.server.config.ServerIndexConfig;
import io.zulia.server.config.ZuliaConfig;
import io.zulia.server.config.cluster.MongoIndexService;
import io.zulia.server.config.single.FSIndexService;
import io.zulia.server.connection.client.InternalClient;
import io.zulia.server.connection.server.validation.CreateIndexRequestValidator;
import io.zulia.server.connection.server.validation.QueryRequestValidator;
import io.zulia.server.exceptions.IndexDoesNotExist;
import io.zulia.server.filestorage.DocumentStorage;
import io.zulia.server.filestorage.FileDocumentStorage;
import io.zulia.server.filestorage.MongoDocumentStorage;
import io.zulia.server.index.federator.ClearRequestFederator;
import io.zulia.server.index.federator.CreateIndexRequestFederator;
import io.zulia.server.index.federator.DeleteIndexRequestFederator;
import io.zulia.server.index.federator.GetFieldNamesRequestFederator;
import io.zulia.server.index.federator.GetNumberOfDocsRequestFederator;
import io.zulia.server.index.federator.GetTermsRequestFederator;
import io.zulia.server.index.federator.OptimizeRequestFederator;
import io.zulia.server.index.federator.QueryRequestFederator;
import io.zulia.server.index.federator.ReindexRequestFederator;
import io.zulia.server.index.router.DeleteRequestRouter;
import io.zulia.server.index.router.FetchRequestRouter;
import io.zulia.server.index.router.StoreRequestRouter;
import io.zulia.server.node.ZuliaNode;
import io.zulia.server.util.MongoProvider;
import io.zulia.util.ZuliaThreadFactory;
import org.apache.lucene.search.Query;
import org.bson.Document;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Writer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ZuliaIndexManager {

	private static final Logger LOG = Logger.getLogger(ZuliaIndexManager.class.getName());

	private final IndexService indexService;
	private final InternalClient internalClient;
	private final ExecutorService pool;
	private final ConcurrentHashMap<String, ZuliaIndex> indexMap;

	private final ZuliaConfig zuliaConfig;
	private final NodeService nodeService;

	private Node thisNode;
	private Collection<Node> currentOtherNodesActive = Collections.emptyList();

	public ZuliaIndexManager(ZuliaConfig zuliaConfig, NodeService nodeService) {

		this.zuliaConfig = zuliaConfig;

		this.thisNode = ZuliaNode.nodeFromConfig(zuliaConfig);
		this.nodeService = nodeService;

		if (zuliaConfig.isCluster()) {
			indexService = new MongoIndexService(MongoProvider.getMongoClient(), zuliaConfig.getClusterName());
		}
		else {
			indexService = new FSIndexService(zuliaConfig);
		}

		this.internalClient = new InternalClient();

		this.indexMap = new ConcurrentHashMap<>();

		this.pool = Executors.newCachedThreadPool(new ZuliaThreadFactory("manager"));

	}

	public void handleNodeAdded(Collection<Node> currentOtherNodesActive, Node nodeAdded) {
		LOG.info(getLogPrefix() + " added node " + nodeAdded.getServerAddress() + ":" + nodeAdded.getServicePort());

		internalClient.addNode(nodeAdded);
		this.currentOtherNodesActive = currentOtherNodesActive;
	}

	private String getLogPrefix() {
		return zuliaConfig.getServerAddress() + ":" + zuliaConfig.getServicePort() + " ";
	}

	public void handleNodeRemoved(Collection<Node> currentOtherNodesActive, Node nodeRemoved) {
		LOG.info(zuliaConfig.getServerAddress() + ":" + zuliaConfig.getServicePort() + " removed node " + nodeRemoved.getServerAddress() + ":" + nodeRemoved
				.getServicePort());
		internalClient.removeNode(nodeRemoved);
		this.currentOtherNodesActive = currentOtherNodesActive;
	}

	public void shutdown() {

		internalClient.close();

		pool.shutdownNow();

		indexMap.values().parallelStream().forEach(zuliaIndex -> {
			try {
				zuliaIndex.unload(false);
			}
			catch (IOException e) {
				LOG.log(Level.SEVERE, "Failed to unload index: " + zuliaIndex.getIndexName(), e);
			}
		});

	}

	public void init() throws Exception {
		List<IndexSettings> indexes = indexService.getIndexes();
		for (IndexSettings indexSettings : indexes) {
			pool.submit(() -> {
				try {
					loadIndex(indexSettings);
				}
				catch (Exception e) {
					LOG.log(Level.SEVERE, e.getClass().getSimpleName() + ":", e);
					throw new RuntimeException(e);
				}
			});
		}

	}

	private void loadIndex(String indexName) throws Exception {

		IndexSettings indexSettings = indexService.getIndex(indexName);

		loadIndex(indexSettings);
	}

	private void loadIndex(IndexSettings indexSettings) throws Exception {
		LOG.info(zuliaConfig.getServerAddress() + ":" + zuliaConfig.getServicePort() + " loading index <" + indexSettings.getIndexName() + ">");

		IndexMapping indexMapping = indexService.getIndexMapping(indexSettings.getIndexName());

		ServerIndexConfig serverIndexConfig = new ServerIndexConfig(indexSettings);

		String dbName = zuliaConfig.getClusterName() + "_" + serverIndexConfig.getIndexName() + "_" + "fs";

		DocumentStorage documentStorage;
		if (zuliaConfig.isCluster()) {
			documentStorage = new MongoDocumentStorage(MongoProvider.getMongoClient(), serverIndexConfig.getIndexName(), dbName, false);
		}
		else {
			documentStorage = new FileDocumentStorage(zuliaConfig, serverIndexConfig.getIndexName());
		}

		ZuliaIndex zuliaIndex = new ZuliaIndex(zuliaConfig, serverIndexConfig, documentStorage, indexService, indexMapping);

		indexMap.put(indexSettings.getIndexName(), zuliaIndex);

		zuliaIndex.loadShards((node) -> ZuliaNode.isEqual(thisNode, node));
	}

	public GetIndexesResponse getIndexes(GetIndexesRequest request) throws Exception {
		GetIndexesResponse.Builder getIndexesResponse = GetIndexesResponse.newBuilder();
		for (IndexSettings indexSettings : indexService.getIndexes()) {
			getIndexesResponse.addIndexName(indexSettings.getIndexName());
		}
		return getIndexesResponse.build();
	}

	public FetchResponse fetch(FetchRequest request) throws Exception {
		ZuliaIndex i = getIndexFromName(request.getIndexName());
		FetchRequestRouter router = new FetchRequestRouter(thisNode, currentOtherNodesActive, request.getMasterSlaveSettings(), i, request.getUniqueId(),
				internalClient);
		return router.send(request);

	}

	public FetchResponse internalFetch(FetchRequest request) throws Exception {
		ZuliaIndex i = getIndexFromName(request.getIndexName());
		return FetchRequestRouter.internalFetch(i, request);
	}

	public ZuliaBase.AssociatedDocument getAssociatedDocument(String indexName, String uniqueId, String fileName) throws Exception {
		ZuliaIndex i = getIndexFromName(indexName);
		return i.getAssociatedDocument(uniqueId, fileName, ZuliaQuery.FetchType.FULL);
	}

	public InputStream getAssociatedDocumentStream(String indexName, String uniqueId, String fileName) throws Exception {
		ZuliaIndex i = getIndexFromName(indexName);
		return i.getAssociatedDocumentStream(uniqueId, fileName);
	}

	public OutputStream getAssociatedDocumentOutputStream(String indexName, String uniqueId, String fileName, Document metadata) throws Exception {
		ZuliaIndex i = getIndexFromName(indexName);
		long timestamp = System.currentTimeMillis();
		return i.getAssociatedDocumentOutputStream(uniqueId, fileName, timestamp, metadata);
	}

	public List<String> getAssociatedFilenames(String indexName, String uniqueId) throws Exception {
		ZuliaIndex i = getIndexFromName(indexName);
		return i.getAssociatedFilenames(uniqueId);
	}

	public void getAssociatedFilenames(String indexName, Writer writer, Document filter) throws Exception {
		ZuliaIndex i = getIndexFromName(indexName);
		i.getAssociatedDocuments(writer, filter);
	}

	public GetNodesResponse getNodes(GetNodesRequest request) throws Exception {

		List<IndexMapping> indexMappingList = indexService.getIndexMappings();
		if ((request.getActiveOnly())) {
			return GetNodesResponse.newBuilder().addAllNode(currentOtherNodesActive).addNode(thisNode).addAllIndexMapping(indexMappingList).build();
		}
		else {
			return GetNodesResponse.newBuilder().addAllNode(nodeService.getNodes()).addAllIndexMapping(indexMappingList).build();
		}
	}

	public InternalQueryResponse internalQuery(InternalQueryRequest request) throws Exception {

		Map<String, Query> queryMap = new HashMap<>();
		Set<ZuliaIndex> indexes = new HashSet<>();

		populateIndexesAndIndexMap(request.getQueryRequest(), queryMap, indexes);

		return QueryRequestFederator.internalQuery(indexes, request, queryMap);
	}

	public QueryResponse query(QueryRequest request) throws Exception {
		request = new QueryRequestValidator().validateAndSetDefault(request);

		Map<String, Query> queryMap = new HashMap<>();
		Set<ZuliaIndex> indexes = new HashSet<>();

		populateIndexesAndIndexMap(request, queryMap, indexes);

		QueryRequestFederator federator = new QueryRequestFederator(thisNode, currentOtherNodesActive, request.getMasterSlaveSettings(), indexes, pool,
				internalClient, queryMap);

		return federator.getResponse(request);
	}

	private void populateIndexesAndIndexMap(QueryRequest queryRequest, Map<String, Query> queryMap, Set<ZuliaIndex> indexes) throws Exception {

		for (String indexName : queryRequest.getIndexList()) {
			ZuliaIndex index = getIndexFromName(indexName);
			indexes.add(index);

			Query query = index.getQuery(queryRequest);
			queryMap.put(indexName, query);
		}
	}

	public StoreResponse store(StoreRequest request) throws Exception {
		ZuliaIndex i = getIndexFromName(request.getIndexName());
		StoreRequestRouter router = new StoreRequestRouter(thisNode, currentOtherNodesActive, i, request.getUniqueId(), internalClient);
		return router.send(request);
	}

	public StoreResponse internalStore(StoreRequest request) throws Exception {
		ZuliaIndex i = getIndexFromName(request.getIndexName());
		return StoreRequestRouter.internalStore(i, request);
	}

	public DeleteResponse delete(DeleteRequest request) throws Exception {
		ZuliaIndex i = getIndexFromName(request.getIndexName());
		DeleteRequestRouter router = new DeleteRequestRouter(thisNode, currentOtherNodesActive, i, request.getUniqueId(), internalClient);
		return router.send(request);
	}

	public DeleteResponse internalDelete(DeleteRequest request) throws Exception {
		ZuliaIndex i = getIndexFromName(request.getIndexName());
		return DeleteRequestRouter.internalDelete(i, request);
	}

	public CreateIndexResponse createIndex(CreateIndexRequest request) throws Exception {
		//if existing index make sure not to allow changing number of shards

		LOG.info(getLogPrefix() + " creating index: " + request);
		request = new CreateIndexRequestValidator().validateAndSetDefault(request);

		if (!request.hasIndexSettings()) {
			throw new IllegalArgumentException("Index settings field is required for create index");
		}

		NodeWeightComputation nodeWeightComputation = new DefaultNodeWeightComputation(indexService, thisNode, currentOtherNodesActive);

		IndexSettings indexSettings = request.getIndexSettings();

		if (indexSettings.getNumberOfShards() == 0) {
			indexSettings = indexSettings.toBuilder().setNumberOfShards(1).build();
		}
		else if (indexSettings.getNumberOfShards() < 0) {
			throw new IllegalArgumentException("Number of shards cannot be negative");
		}

		String indexName = indexSettings.getIndexName();
		IndexSettings existingIndex = indexService.getIndex(indexName);

		long currentTimeMillis = System.currentTimeMillis();
		if (existingIndex == null) {

			indexSettings = indexSettings.toBuilder().setCreateTime(currentTimeMillis).build();

			IndexMapping.Builder indexMapping = IndexMapping.newBuilder();
			indexMapping.setIndexName(indexName);
			indexMapping.setNumberOfShards(indexSettings.getNumberOfShards());

			for (int i = 0; i < indexSettings.getNumberOfShards(); i++) {

				List<Node> nodes = nodeWeightComputation.getNodesSortedByWeight();

				ShardMapping.Builder shardMapping = ShardMapping.newBuilder();

				Node primaryNode = nodes.remove(0);
				shardMapping.setPrimaryNode(primaryNode);
				shardMapping.setShardNumber(i);
				nodeWeightComputation.addShard(primaryNode, indexSettings, true);

				for (int r = 0; r < nodes.size(); r++) {
					if (r < indexSettings.getNumberOfReplicas()) {
						Node replicaNode = nodes.get(r);
						shardMapping.addReplicaNode(replicaNode);
						nodeWeightComputation.addShard(replicaNode, indexSettings, false);
					}
					else {
						break;
					}
				}

				indexMapping.addShardMapping(shardMapping);
			}

			indexService.storeIndexMapping(indexMapping.build());
		}
		else {

			if (existingIndex.equals(indexSettings)) {
				LOG.info("No changes to existing index <" + indexName + ">");
				return CreateIndexResponse.newBuilder().build();
			}

			if (existingIndex.getNumberOfShards() != indexSettings.getNumberOfShards()) {
				throw new IllegalArgumentException("Cannot change shards for existing index");
			}

			//TODO handle changing of replication factor
			if (existingIndex.getNumberOfReplicas() != indexSettings.getNumberOfReplicas()) {
				throw new IllegalArgumentException("Cannot change replication factor for existing index yet");
			}

		}
		indexSettings = indexSettings.toBuilder().setUpdateTime(currentTimeMillis).build();
		indexService.createIndex(indexSettings);

		CreateIndexRequestFederator createIndexRequestFederator = new CreateIndexRequestFederator(thisNode, currentOtherNodesActive, pool, internalClient,
				this);

		try {
			List<CreateIndexResponse> send = createIndexRequestFederator.send(InternalCreateIndexRequest.newBuilder().setIndexName(indexName).build());
		}
		catch (Exception e) {
			if (existingIndex == null) {
				LOG.log(Level.SEVERE, "Failed to update index <" + request.getIndexSettings().getIndexName() + ">: ", e);
				throw new Exception("Failed to update index <" + request.getIndexSettings().getIndexName() + ">: " + e.getMessage());
			}
			else {
				throw new Exception("Failed to create index <" + request.getIndexSettings().getIndexName() + ">: " + e.getMessage());

			}
		}

		return CreateIndexResponse.newBuilder().build();
	}

	public CreateIndexResponse internalCreateIndex(String indexName) throws Exception {

		ZuliaIndex zuliaIndex = indexMap.get(indexName);
		if (zuliaIndex == null) {
			loadIndex(indexName);
		}
		else {
			zuliaIndex.reloadIndexSettings();
		}

		return CreateIndexResponse.newBuilder().build();

	}

	public DeleteIndexResponse deleteIndex(DeleteIndexRequest request) throws Exception {
		LOG.info(getLogPrefix() + "Received delete index request for <" + request.getIndexName() + ">");

		DeleteIndexRequestFederator deleteIndexRequestFederator = new DeleteIndexRequestFederator(thisNode, currentOtherNodesActive, pool, internalClient,
				this);

		List<DeleteIndexResponse> response = deleteIndexRequestFederator.send(request);

		String indexName = request.getIndexName();
		indexService.removeIndex(indexName);
		indexService.removeIndexMapping(indexName);

		return DeleteIndexResponse.newBuilder().build();

	}

	public DeleteIndexResponse internalDeleteIndex(DeleteIndexRequest request) throws Exception {
		String indexName = request.getIndexName();
		ZuliaIndex zuliaIndex = indexMap.get(indexName);
		if (zuliaIndex != null) {
			LOG.info(getLogPrefix() + "Deleting index <" + request.getIndexName() + ">");
			//zuliaIndex.unload(true);
			zuliaIndex.deleteIndex();
			indexMap.remove(indexName);
			LOG.info(getLogPrefix() + "Deleted index <" + request.getIndexName() + ">");
		}
		else {
			LOG.info(getLogPrefix() + "Index <" + request.getIndexName() + "> was not found");
		}
		return DeleteIndexResponse.newBuilder().build();
	}

	public GetNumberOfDocsResponse getNumberOfDocs(GetNumberOfDocsRequest request) throws Exception {
		ZuliaIndex i = getIndexFromName(request.getIndexName());
		GetNumberOfDocsRequestFederator federator = new GetNumberOfDocsRequestFederator(thisNode, currentOtherNodesActive, MasterSlaveSettings.MASTER_ONLY, i,
				pool, internalClient);
		return federator.getResponse(request);

	}

	public GetNumberOfDocsResponse getNumberOfDocsInternal(InternalGetNumberOfDocsRequest internalRequest) throws Exception {
		ZuliaIndex i = getIndexFromName(internalRequest.getGetNumberOfDocsRequest().getIndexName());
		return GetNumberOfDocsRequestFederator.internalGetNumberOfDocs(i, internalRequest);
	}

	public ClearResponse clear(ClearRequest request) throws Exception {
		ZuliaIndex i = getIndexFromName(request.getIndexName());
		ClearRequestFederator federator = new ClearRequestFederator(thisNode, currentOtherNodesActive, MasterSlaveSettings.MASTER_ONLY, i, pool,
				internalClient);
		return federator.getResponse(request);
	}

	public ClearResponse internalClear(ClearRequest request) throws Exception {
		ZuliaIndex i = getIndexFromName(request.getIndexName());
		return ClearRequestFederator.internalClear(i, request);
	}

	public OptimizeResponse optimize(OptimizeRequest request) throws Exception {

		ZuliaIndex i = getIndexFromName(request.getIndexName());
		OptimizeRequestFederator federator = new OptimizeRequestFederator(thisNode, currentOtherNodesActive, MasterSlaveSettings.MASTER_ONLY, i, pool,
				internalClient);
		return federator.getResponse(request);
	}

	public OptimizeResponse internalOptimize(OptimizeRequest request) throws Exception {
		ZuliaIndex i = getIndexFromName(request.getIndexName());
		return OptimizeRequestFederator.internalOptimize(i, request);
	}

	public ReindexResponse reindex(ReindexRequest request) throws Exception {
		ZuliaIndex i = getIndexFromName(request.getIndexName());
		ReindexRequestFederator federator = new ReindexRequestFederator(thisNode, currentOtherNodesActive, MasterSlaveSettings.MASTER_ONLY, i, pool,
				internalClient);
		return federator.getResponse(request);
	}

	public ReindexResponse internalReindex(ReindexRequest request) throws Exception {
		ZuliaIndex i = getIndexFromName(request.getIndexName());
		return ReindexRequestFederator.internalReindex(i, request);
	}

	public GetFieldNamesResponse getFieldNames(GetFieldNamesRequest request) throws Exception {
		MasterSlaveSettings masterSlaveSettings = request.getMasterSlaveSettings();
		ZuliaIndex i = getIndexFromName(request.getIndexName());
		GetFieldNamesRequestFederator federator = new GetFieldNamesRequestFederator(thisNode, currentOtherNodesActive, masterSlaveSettings, i, pool,
				internalClient);
		return federator.getResponse(request);

	}

	public GetFieldNamesResponse internalGetFieldNames(InternalGetFieldNamesRequest request) throws Exception {
		GetFieldNamesRequest getFieldNamesRequest = request.getGetFieldNamesRequest();
		ZuliaIndex i = getIndexFromName(getFieldNamesRequest.getIndexName());
		return GetFieldNamesRequestFederator.internalGetFieldNames(i, request);
	}

	public GetTermsResponse getTerms(GetTermsRequest request) throws Exception {
		ZuliaIndex i = getIndexFromName(request.getIndexName());
		GetTermsRequestFederator federator = new GetTermsRequestFederator(thisNode, currentOtherNodesActive, request.getMasterSlaveSettings(), i, pool,
				internalClient);
		return federator.getResponse(request);

	}

	public InternalGetTermsResponse internalGetTerms(InternalGetTermsRequest request) throws Exception {
		GetTermsRequest getTermsRequest = request.getGetTermsRequest();
		ZuliaIndex i = getIndexFromName(getTermsRequest.getIndexName());
		return GetTermsRequestFederator.internalGetTerms(i, request);
	}

	public GetIndexSettingsResponse getIndexSettings(GetIndexSettingsRequest request) throws Exception {
		ZuliaIndex i = getIndexFromName(request.getIndexName());
		return GetIndexSettingsResponse.newBuilder().setIndexSettings(i.getIndexConfig().getIndexSettings()).build();
	}

	private ZuliaIndex getIndexFromName(String indexName) throws IndexDoesNotExist {
		ZuliaIndex i = indexMap.get(indexName);
		if (i == null) {
			throw new IndexDoesNotExist(indexName);
		}
		return i;
	}

}
