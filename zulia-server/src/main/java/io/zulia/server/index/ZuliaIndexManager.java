package io.zulia.server.index;

import io.zulia.message.ZuliaBase.MasterSlaveSettings;
import io.zulia.message.ZuliaBase.Node;
import io.zulia.message.ZuliaIndex.IndexMapping;
import io.zulia.message.ZuliaIndex.IndexSettings;
import io.zulia.message.ZuliaServiceOuterClass.*;
import io.zulia.server.config.IndexService;
import io.zulia.server.config.NodeService;
import io.zulia.server.config.ServerIndexConfig;
import io.zulia.server.config.ZuliaConfig;
import io.zulia.server.config.cluster.MongoIndexService;
import io.zulia.server.config.single.FSIndexService;
import io.zulia.server.connection.client.InternalClient;
import io.zulia.server.exceptions.IndexDoesNotExist;
import io.zulia.server.filestorage.DocumentStorage;
import io.zulia.server.filestorage.FileDocumentStorage;
import io.zulia.server.filestorage.MongoDocumentStorage;
import io.zulia.server.index.federator.ClearRequestNodeFederator;
import io.zulia.server.index.federator.GetFieldNamesRequestNodeFederator;
import io.zulia.server.index.federator.GetNumberOfDocsRequestNodeFederator;
import io.zulia.server.index.federator.GetTermsRequestNodeFederator;
import io.zulia.server.index.federator.OptimizeRequestNodeFederator;
import io.zulia.server.index.federator.QueryRequestNodeFederator;
import io.zulia.server.index.router.DeleteRequestNodeRouter;
import io.zulia.server.index.router.FetchRequestNodeRouter;
import io.zulia.server.index.router.StoreRequestNodeRouter;
import io.zulia.server.node.ZuliaNode;
import io.zulia.server.util.MongoProvider;
import io.zulia.util.ZuliaThreadFactory;
import org.apache.lucene.search.Query;
import org.bson.Document;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
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
	private Collection<Node> currentOtherNodesActive;

	public ZuliaIndexManager(ZuliaConfig zuliaConfig, NodeService nodeService) throws Exception {

		this.zuliaConfig = zuliaConfig;

		this.thisNode = ZuliaNode.nodeFromConfig(zuliaConfig);
		this.nodeService = nodeService;

		if (zuliaConfig.isCluster()) {
			indexService = new MongoIndexService(MongoProvider.getMongoClient());
		}
		else {
			indexService = new FSIndexService(zuliaConfig);
		}

		this.internalClient = new InternalClient();

		this.indexMap = new ConcurrentHashMap<>();

		this.pool = Executors.newCachedThreadPool(new ZuliaThreadFactory("manager"));

	}

	public void handleNodeAdded(Collection<Node> currentOtherNodesActive, Node nodeAdded) {
		LOG.info("nodeAdded: " + nodeAdded.getServerAddress() + ":" + nodeAdded.getServicePort());

		internalClient.addNode(nodeAdded);
		this.currentOtherNodesActive = currentOtherNodesActive;
	}

	public void handleNodeRemoved(Collection<Node> currentOtherNodesActive, Node nodeRemoved) {
		LOG.info("nodeRemoved: " + nodeRemoved.getServerAddress() + ":" + nodeRemoved.getServicePort());
		internalClient.removeNode(nodeRemoved);
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
			loadIndex(indexSettings);
		}

	}

	private void loadIndex(IndexSettings indexSettings) throws Exception {
		IndexMapping indexMapping = indexService.getIndexMapping(indexSettings.getIndexName());

		pool.submit(() -> {

			ServerIndexConfig serverIndexConfig = new ServerIndexConfig(indexSettings);

			String dbName = zuliaConfig.getClusterName() + "_" + serverIndexConfig.getIndexName() + "_" + "fs";

			DocumentStorage documentStorage;
			if (zuliaConfig.isCluster()) {
				documentStorage = new MongoDocumentStorage(MongoProvider.getMongoClient(), serverIndexConfig.getIndexName(), dbName, false);
			}
			else {
				documentStorage = new FileDocumentStorage(zuliaConfig, serverIndexConfig.getIndexName());
			}

			ZuliaIndex zuliaIndex = new ZuliaIndex(serverIndexConfig, documentStorage, indexService);
			zuliaIndex.setIndexMapping(indexMapping);

			indexMap.put(indexSettings.getIndexName(), zuliaIndex);

			zuliaIndex.loadShards();
		});
	}

	public GetIndexesResponse getIndexes(GetIndexesRequest request) throws Exception {
		//TODO: switch to use what is cached vs. in DB
		GetIndexesResponse.Builder getIndexesResponse = GetIndexesResponse.newBuilder();
		for (IndexSettings indexSettings : indexService.getIndexes()) {
			getIndexesResponse.addIndexName(indexSettings.getIndexName());
		}
		return getIndexesResponse.build();
	}

	public FetchResponse fetch(FetchRequest request) throws Exception {
		ZuliaIndex i = getIndexFromName(request.getIndexName());
		FetchRequestNodeRouter router = new FetchRequestNodeRouter(thisNode, currentOtherNodesActive, request.getMasterSlaveSettings(), i,
				request.getUniqueId(), internalClient);
		return router.send(request);

	}

	public FetchResponse internalFetch(FetchRequest request) throws Exception {
		ZuliaIndex i = getIndexFromName(request.getIndexName());
		return FetchRequestNodeRouter.internalFetch(i, request);
	}

	public InputStream getAssociatedDocumentStream(String indexName, String uniqueId, String fileName) throws IOException {
		ZuliaIndex i = getIndexFromName(indexName);
		return i.getAssociatedDocumentStream(uniqueId, fileName);
	}

	public void storeAssociatedDocument(String indexName, String uniqueId, String fileName, InputStream is, HashMap<String, String> metadataMap)
			throws Exception {
		ZuliaIndex i = getIndexFromName(indexName);
		long timestamp = System.currentTimeMillis();
		i.storeAssociatedDocument(uniqueId, fileName, is, timestamp, metadataMap);

	}

	public void getAssociatedDocuments(String indexName, OutputStream outputStream, Document filter) throws IOException {
		ZuliaIndex i = getIndexFromName(indexName);
		i.getAssociatedDocuments(outputStream, filter);
	}

	public GetNodesResponse getNodes(GetNodesRequest request) {
		if ((request.getActiveOnly())) {
			return GetNodesResponse.newBuilder().addAllNode(currentOtherNodesActive).build();
		}
		else {
			return GetNodesResponse.newBuilder().addAllNode(nodeService.getNodes()).build();
		}
	}

	public InternalQueryResponse internalQuery(QueryRequest request) throws Exception {

		Map<String, Query> queryMap = new HashMap<>();
		Set<ZuliaIndex> indexes = new HashSet<>();

		populateIndexesAndIndexMap(request, queryMap, indexes);

		return QueryRequestNodeFederator.internalQuery(indexes, request, queryMap);
	}

	public QueryResponse query(QueryRequest request) throws Exception {
		Map<String, Query> queryMap = new HashMap<>();
		Set<ZuliaIndex> indexes = new HashSet<>();

		populateIndexesAndIndexMap(request, queryMap, indexes);

		QueryRequestNodeFederator federator = new QueryRequestNodeFederator(thisNode, currentOtherNodesActive, request.getMasterSlaveSettings(), indexes, pool,
				internalClient, queryMap);

		return federator.getResponse(request);
	}

	private void populateIndexesAndIndexMap(QueryRequest request, Map<String, Query> queryMap, Set<ZuliaIndex> indexes) throws Exception {
		for (String indexName : request.getIndexList()) {
			ZuliaIndex index = getIndexFromName(indexName);
			indexes.add(index);

			Query query = index.getQuery(request);
			queryMap.put(indexName, query);
		}
	}

	public StoreResponse store(StoreRequest request) throws Exception {
		ZuliaIndex i = getIndexFromName(request.getIndexName());
		StoreRequestNodeRouter router = new StoreRequestNodeRouter(thisNode, currentOtherNodesActive, MasterSlaveSettings.MASTER_ONLY, i, request.getUniqueId(),
				internalClient);
		return router.send(request);
	}

	public StoreResponse internalStore(StoreRequest request) throws Exception {
		ZuliaIndex i = getIndexFromName(request.getIndexName());
		return StoreRequestNodeRouter.internalStore(i, request);
	}

	public DeleteResponse delete(DeleteRequest request) throws Exception {
		ZuliaIndex i = getIndexFromName(request.getIndexName());
		DeleteRequestNodeRouter router = new DeleteRequestNodeRouter(thisNode, currentOtherNodesActive, MasterSlaveSettings.MASTER_ONLY, i,
				request.getUniqueId(), internalClient);
		return router.send(request);
	}

	public DeleteResponse internalDelete(DeleteRequest request) throws Exception {
		ZuliaIndex i = getIndexFromName(request.getIndexName());
		return DeleteRequestNodeRouter.internalDelete(i, request);
	}

	public CreateIndexResponse createIndex(CreateIndexRequest request) throws Exception {
		//if existing index make sure not to allow changing number of shards

		IndexSettings existingIndex = indexService.getIndex(request.getIndexSettings().getIndexName());

		if (existingIndex == null) {

			indexService.createIndex(request.getIndexSettings());
		}
		else {

			if (existingIndex.getNumberOfShards() != request.getIndexSettings().getNumberOfShards()) {
				throw new IllegalArgumentException("Cannot change shards for existing index");
			}

			indexService.createIndex(request.getIndexSettings());
		}

		//index mapping

		//balance shards / indexes

		//load shards / index

		//tell others

		return CreateIndexResponse.newBuilder().build();
	}

	public DeleteIndexResponse deleteIndex(DeleteIndexRequest request) {
		return null;
	}

	public GetNumberOfDocsResponse getNumberOfDocs(GetNumberOfDocsRequest request) throws Exception {
		ZuliaIndex i = getIndexFromName(request.getIndexName());
		GetNumberOfDocsRequestNodeFederator federator = new GetNumberOfDocsRequestNodeFederator(thisNode, currentOtherNodesActive,
				MasterSlaveSettings.MASTER_ONLY, i, pool, internalClient);
		return federator.getResponse(request);

	}

	public GetNumberOfDocsResponse getNumberOfDocsInternal(GetNumberOfDocsRequest request) throws Exception {
		ZuliaIndex i = getIndexFromName(request.getIndexName());
		return GetNumberOfDocsRequestNodeFederator.internalGetNumberOfDocs(i, request);
	}

	public ClearResponse clear(ClearRequest request) throws Exception {
		ZuliaIndex i = getIndexFromName(request.getIndexName());
		ClearRequestNodeFederator federator = new ClearRequestNodeFederator(thisNode, currentOtherNodesActive, MasterSlaveSettings.MASTER_ONLY, i, pool,
				internalClient);
		return federator.getResponse(request);
	}

	public ClearResponse internalClear(ClearRequest request) throws Exception {
		ZuliaIndex i = getIndexFromName(request.getIndexName());
		return ClearRequestNodeFederator.internalClear(i, request);
	}

	public OptimizeResponse optimize(OptimizeRequest request) throws Exception {

		ZuliaIndex i = getIndexFromName(request.getIndexName());
		OptimizeRequestNodeFederator federator = new OptimizeRequestNodeFederator(thisNode, currentOtherNodesActive, MasterSlaveSettings.MASTER_ONLY, i, pool,
				internalClient);
		return federator.getResponse(request);
	}

	public OptimizeResponse internalOptimize(OptimizeRequest request) throws Exception {
		ZuliaIndex i = getIndexFromName(request.getIndexName());
		i.optimize(request);
		return OptimizeRequestNodeFederator.internalOptimize(i, request);
	}

	public GetFieldNamesResponse getFieldNames(GetFieldNamesRequest request) throws Exception {
		MasterSlaveSettings masterSlaveSettings = request.getMasterSlaveSettings();
		ZuliaIndex i = getIndexFromName(request.getIndexName());
		GetFieldNamesRequestNodeFederator federator = new GetFieldNamesRequestNodeFederator(thisNode, currentOtherNodesActive, masterSlaveSettings, i, pool,
				internalClient);
		return federator.getResponse(request);

	}

	public GetFieldNamesResponse internalGetFieldNames(GetFieldNamesRequest request) throws Exception {
		ZuliaIndex i = getIndexFromName(request.getIndexName());
		return GetFieldNamesRequestNodeFederator.internalGetFieldNames(i, request);
	}

	public GetTermsResponse getTerms(GetTermsRequest request) throws Exception {
		ZuliaIndex i = getIndexFromName(request.getIndexName());
		GetTermsRequestNodeFederator federator = new GetTermsRequestNodeFederator(thisNode, currentOtherNodesActive, request.getMasterSlaveSettings(), i, pool,
				internalClient);
		return federator.getResponse(request);

	}

	public InternalGetTermsResponse internalGetTerms(GetTermsRequest request) throws Exception {
		ZuliaIndex i = getIndexFromName(request.getIndexName());
		return GetTermsRequestNodeFederator.internalGetTerms(i, request);
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
