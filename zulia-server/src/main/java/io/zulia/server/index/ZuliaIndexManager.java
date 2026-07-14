package io.zulia.server.index;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;
import io.grpc.stub.StreamObserver;
import io.zulia.message.ZuliaBase;
import io.zulia.message.ZuliaBase.IndexStats;
import io.zulia.message.ZuliaBase.Node;
import io.zulia.message.ZuliaBase.PrimaryReplicaSettings;
import io.zulia.message.ZuliaIndex.AnalyzerSettings;
import io.zulia.message.ZuliaIndex.FieldConfig;
import io.zulia.message.ZuliaIndex.FieldMapping;
import io.zulia.message.ZuliaIndex.IndexAlias;
import io.zulia.message.ZuliaIndex.IndexSettings;
import io.zulia.message.ZuliaIndex.IndexShardMapping;
import io.zulia.message.ZuliaIndex.ShardMapping;
import io.zulia.message.ZuliaIndex.UpdateIndexSettings;
import io.zulia.message.ZuliaIndex.UpdateIndexSettings.Operation;
import io.zulia.message.ZuliaIndex.UpdateIndexSettings.Operation.OperationType;
import io.zulia.message.ZuliaQuery;
import io.zulia.message.ZuliaQuery.FetchType;
import io.zulia.message.ZuliaServiceOuterClass.*;
import io.zulia.rest.dto.AssociatedMetadataDTO;
import io.zulia.server.config.IndexFieldInfo;
import io.zulia.server.config.IndexService;
import io.zulia.server.config.NodeService;
import io.zulia.server.config.ServerIndexConfig;
import io.zulia.server.config.ZuliaConfig;
import io.zulia.server.config.cluster.MongoIndexService;
import io.zulia.server.config.single.FSIndexService;
import io.zulia.server.connection.client.InternalClient;
import io.zulia.server.connection.server.validation.CreateIndexRequestValidator;
import io.zulia.server.connection.server.validation.QueryRequestValidator;
import io.zulia.server.exceptions.IndexDoesNotExistException;
import io.zulia.server.field.FieldTypeUtil;
import io.zulia.server.filestorage.DocumentStorage;
import io.zulia.server.filestorage.FileDocumentStorage;
import io.zulia.server.filestorage.MongoDocumentStorage;
import io.zulia.server.filestorage.S3DocumentStorage;
import io.zulia.server.index.federator.BatchDeleteRequestFederator;
import io.zulia.server.index.federator.BatchFetchRequestFederator;
import io.zulia.server.index.federator.ClearRequestFederator;
import io.zulia.server.index.federator.CreateIndexAliasRequestFederator;
import io.zulia.server.index.federator.CreateOrUpdateIndexRequestFederator;
import io.zulia.server.index.federator.DeleteIndexAliasRequestFederator;
import io.zulia.server.index.federator.DeleteIndexRequestFederator;
import io.zulia.server.index.federator.GetFieldNamesRequestFederator;
import io.zulia.server.index.federator.GetNumberOfDocsRequestFederator;
import io.zulia.server.index.federator.GetTermsRequestFederator;
import io.zulia.server.index.federator.OptimizeRequestFederator;
import io.zulia.server.index.federator.QueryRequestFederator;
import io.zulia.server.index.federator.ReindexRequestFederator;
import io.zulia.server.index.replication.ReplicationRateLimiter;
import io.zulia.server.index.replication.ReplicationShardState;
import io.zulia.server.index.resident.IndexLease;
import io.zulia.server.index.resident.IndexLeases;
import io.zulia.server.index.resident.LoadedIndexCache;
import io.zulia.server.index.resident.RegisteredIndex;
import io.zulia.server.index.resident.TransientIndexPolicy;
import io.zulia.server.index.router.DeleteRequestRouter;
import io.zulia.server.index.router.FetchRequestRouter;
import io.zulia.server.index.router.StoreRequestRouter;
import io.zulia.server.node.ZuliaNode;
import io.zulia.server.util.DeletingFileVisitor;
import io.zulia.server.util.MongoProvider;
import io.zulia.util.IndexAliasUtil;
import io.zulia.util.ResultHelper;
import io.zulia.util.ZuliaUtil;
import io.zulia.util.document.DocumentHelper;
import io.zulia.util.pool.TaskExecutor;
import io.zulia.util.pool.WorkPool;
import org.apache.lucene.search.Query;
import org.bson.Document;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ZuliaIndexManager {

	private final static Logger LOG = LoggerFactory.getLogger(ZuliaIndexManager.class);
	private final IndexService indexService;
	private final InternalClient internalClient;
	private final ExecutorService pool;
	private final LoadedIndexCache loadedIndexCache;
	private final ZuliaConfig zuliaConfig;
	private final NodeService nodeService;
	private final Node thisNode;
	private volatile Collection<Node> currentOtherNodesActive = Collections.emptyList();
	private final ConcurrentHashMap<String, IndexAlias> indexAliasMap;
	private final ReplicationRateLimiter replicationRateLimiter;

	private static final int MONGO_DB_NAME_MAX_LENGTH = 63;
	private static final int MLT_MAX_SOURCE_DOCS = 100;

	public ZuliaIndexManager(ZuliaConfig zuliaConfig, NodeService nodeService) throws Exception {

		this.zuliaConfig = zuliaConfig;
		this.replicationRateLimiter = new ReplicationRateLimiter(zuliaConfig.getReplicationMaxBytesPerSec());

		this.thisNode = ZuliaNode.nodeFromConfig(zuliaConfig);
		this.nodeService = nodeService;

		if (zuliaConfig.isCluster()) {
			indexService = new MongoIndexService(MongoProvider.getMongoClient(), zuliaConfig.getClusterName());
		}
		else {
			indexService = new FSIndexService(zuliaConfig);
		}

		this.internalClient = new InternalClient();

		TransientIndexPolicy policy = TransientIndexPolicy.fromConfig(zuliaConfig);
		this.loadedIndexCache = new LoadedIndexCache(policy, this::loadIndexForCache);
		this.indexAliasMap = new ConcurrentHashMap<>();

		for (IndexAlias indexAlias : indexService.getIndexAliases()) {
			indexAliasMap.put(indexAlias.getAliasName(), indexAlias);
		}

		this.pool = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("manager-", 0).factory());

	}

	public void handleNodeAdded(Collection<Node> currentOtherNodesActive, Node nodeAdded) {
		LOG.info("{}:{} added node {}:{}", zuliaConfig.getServerAddress(), zuliaConfig.getServicePort(), nodeAdded.getServerAddress(),
				nodeAdded.getServicePort());

		internalClient.addNode(nodeAdded);
		this.currentOtherNodesActive = currentOtherNodesActive;
	}

	public void handleNodeRemoved(Collection<Node> currentOtherNodesActive, Node nodeRemoved) {
		LOG.info("{}:{} removed node {}:{}", zuliaConfig.getServerAddress(), zuliaConfig.getServicePort(), nodeRemoved.getServerAddress(),
				nodeRemoved.getServicePort());
		internalClient.removeNode(nodeRemoved);
		this.currentOtherNodesActive = currentOtherNodesActive;
	}

	public void shutdown() {

		internalClient.close();

		pool.shutdownNow();

		loadedIndexCache.shutdown();

	}

	public void init() throws Exception {
		List<IndexSettings> indexes = indexService.getIndexes();

		Map<String, IndexShardMapping> mappingByName = new HashMap<>();
		try {
			for (IndexShardMapping indexShardMapping : indexService.getIndexShardMappings()) {
				mappingByName.put(indexShardMapping.getIndexName(), indexShardMapping);
			}
		}
		catch (Exception e) {
			LOG.error("Failed to bulk read shard mappings, falling back to per-index reads", e);
		}

		List<String> eagerLoad = new ArrayList<>();
		for (IndexSettings indexSettings : indexes) {
			String indexName = indexSettings.getIndexName();
			IndexShardMapping mapping = mappingByName.get(indexName);
			if (mapping == null) {
				try {
					mapping = indexService.getIndexShardMapping(indexName);
				}
				catch (Exception e) {
					LOG.error("Failed to read shard mapping for index {}", indexName, e);
				}
			}
			if (mapping == null) {
				// an index without a readable mapping cannot be routed or loaded, so skip it instead of failing startup
				LOG.error("Index {} has no shard mapping and will not be registered", indexName);
				continue;
			}
			loadedIndexCache.register(new RegisteredIndex(indexName, mapping));
			if (indexSettings.getTransientIndex()) {
				LOG.info("{}:{} registered transient index {} for on-demand loading", zuliaConfig.getServerAddress(), zuliaConfig.getServicePort(), indexName);
			}
			else {
				eagerLoad.add(indexName);
			}
		}

		try (TaskExecutor initPool = WorkPool.nativePool(Runtime.getRuntime().availableProcessors(), "index-init")) {
			for (String indexName : eagerLoad) {
				initPool.execute(() -> {
					try (IndexLease _ = loadedIndexCache.leaseOrLoad(indexName)) {
						// load only, the index stays resident after the lease is released
					}
					catch (Exception e) {
						LOG.error("{}:", e.getClass().getSimpleName(), e);
						throw new RuntimeException(e);
					}
				});
			}
		}
	}

	// builds a fully started index for the cache but never touches cache itself
	private ZuliaIndex loadIndexForCache(String indexName) throws Exception {
		IndexSettings indexSettings = indexService.getIndex(indexName);

		if (indexSettings == null) {
			throw new IndexDoesNotExistException(indexName);
		}

		LOG.info("{}:{} loading index {}", zuliaConfig.getServerAddress(), zuliaConfig.getServicePort(), indexName);

		IndexShardMapping currentMapping = indexService.getIndexShardMapping(indexName);

		// Load with the last mapping this node applied, then run the transitions to the current one, so a
		// fault-in racing a mapping update cannot skip the on-disk cleanup for roles this node lost.
		RegisteredIndex registered = loadedIndexCache.getRegistered(indexName);
		IndexShardMapping previouslyApplied = registered != null && registered.shardMapping() != null ? registered.shardMapping() : currentMapping;

		ServerIndexConfig serverIndexConfig = new ServerIndexConfig(indexSettings);

		DocumentStorage documentStorage = getDocumentStorage(serverIndexConfig);

		ZuliaIndex zuliaIndex = new ZuliaIndex(zuliaConfig, serverIndexConfig, documentStorage, indexService, previouslyApplied, internalClient,
				replicationRateLimiter);

		try {
			zuliaIndex.loadShards((node) -> ZuliaNode.isEqual(thisNode, node));

			if (!previouslyApplied.equals(currentMapping)) {
				LOG.info("{}:{} applying shard mapping update for index {} during load", zuliaConfig.getServerAddress(), zuliaConfig.getServicePort(),
						indexName);
				zuliaIndex.applyShardMappingUpdate(currentMapping, node -> ZuliaNode.isEqual(thisNode, node));
			}

			zuliaIndex.startMaintenance();
		}
		catch (Exception e) {
			// close whatever shards opened before the failure, or their writers hold the shard write.lock
			// in-process and every retry of this load fails until a JVM restart
			try {
				zuliaIndex.unload(false);
			}
			catch (Exception cleanup) {
				e.addSuppressed(cleanup);
			}
			throw e;
		}

		return zuliaIndex;
	}

	@NotNull
	private DocumentStorage getDocumentStorage(ServerIndexConfig serverIndexConfig) {

		DocumentStorage documentStorage;
		if (zuliaConfig.isCluster()) {
			String dbName = zuliaConfig.getClusterName() + "_" + serverIndexConfig.getIndexName() + "_" + "fs";

			if (dbName.length() > MONGO_DB_NAME_MAX_LENGTH) {
				LOG.warn(
						"Associated document storage for index <{}> is unavailable: MongoDB database name <{}> exceeds {} character limit ({} characters). Associated document operations will fail for this index.",
						serverIndexConfig.getIndexName(), dbName, MONGO_DB_NAME_MAX_LENGTH, dbName.length());
			}

			documentStorage = switch (zuliaConfig.getClusterStorageEngine()) {
				case "s3" -> new S3DocumentStorage(MongoProvider.getMongoClient(), serverIndexConfig.getIndexName(), dbName, false, zuliaConfig.getS3());
				default -> new MongoDocumentStorage(MongoProvider.getMongoClient(), serverIndexConfig.getIndexName(), dbName, false);
			};
		}
		else {
			documentStorage = new FileDocumentStorage(zuliaConfig, serverIndexConfig.getIndexName());
		}
		return documentStorage;
	}

	public GetIndexesResponse getIndexes(@SuppressWarnings("unused") GetIndexesRequest request) throws Exception {
		GetIndexesResponse.Builder getIndexesResponse = GetIndexesResponse.newBuilder();
		for (IndexSettings indexSettings : indexService.getIndexes()) {
			getIndexesResponse.addIndexName(indexSettings.getIndexName());
		}
		return getIndexesResponse.build();
	}

	public FetchResponse fetch(FetchRequest request) throws Exception {
		try (IndexLease lease = leaseIndexFromName(request.getIndexName())) {
			ZuliaIndex i = lease.getIndex();
			FetchRequestRouter router = new FetchRequestRouter(thisNode, currentOtherNodesActive, request.getPrimaryReplicaSettings(), i, request.getUniqueId(),
					internalClient);
			return router.send(request);
		}
	}

	public FetchResponse internalFetch(FetchRequest request) throws Exception {
		try (IndexLease lease = leaseIndexFromName(request.getIndexName())) {
			ZuliaIndex i = lease.getIndex();
			return FetchRequestRouter.internalFetch(i, request);
		}
	}

	public void batchFetch(BatchFetchRequest request, StreamObserver<FetchResponse> responseObserver) throws Exception {
		if (request.getFetchRequestList().isEmpty() && request.getBatchFetchGroupList().isEmpty()) {
			return;
		}

		long startTime = System.currentTimeMillis();

		Set<String> indexNames = new HashSet<>();
		int requestedIds = 0;
		for (FetchRequest fetchRequest : request.getFetchRequestList()) {
			indexNames.add(fetchRequest.getIndexName());
			requestedIds++;
		}
		for (BatchFetchGroup group : request.getBatchFetchGroupList()) {
			indexNames.add(group.getIndexName());
			requestedIds += group.getUniqueIdCount();
		}

		try (IndexLeases leases = IndexLeases.acquire(indexNames, this::leaseIndexFromName)) {
			Map<String, ZuliaIndex> indexCache = leases.getIndexes();
			BatchFetchRequestFederator federator = new BatchFetchRequestFederator(thisNode, currentOtherNodesActive, pool, internalClient, indexCache);
			List<FetchResponse> responses = federator.send(request);
			for (FetchResponse response : responses) {
				responseObserver.onNext(response);
			}

			long endTime = System.currentTimeMillis();
			LOG.info("Batch fetch for index(es) {}: requested <{}>, retrieved <{}> in {}ms", indexNames, requestedIds, responses.size(), (endTime - startTime));
		}
	}

	public List<FetchResponse> internalBatchFetch(InternalBatchFetchRequest request) throws Exception {
		List<InternalShardBatchFetchRequest> shardRequests = request.getShardBatchFetchRequestList();
		if (shardRequests.size() == 1) {
			try (IndexLease lease = leaseIndexFromName(shardRequests.getFirst().getIndexName())) {
				ZuliaIndex index = lease.getIndex();
				return index.internalShardBatchFetch(shardRequests.getFirst());
			}
		}

		List<Future<List<FetchResponse>>> futures = new ArrayList<>(shardRequests.size());
		for (InternalShardBatchFetchRequest shardRequest : shardRequests) {
			futures.add(pool.submit(() -> {
				try (IndexLease lease = leaseIndexFromName(shardRequest.getIndexName())) {
					ZuliaIndex index = lease.getIndex();
					return index.internalShardBatchFetch(shardRequest);
				}
			}));
		}

		List<FetchResponse> allResponses = new ArrayList<>();
		for (Future<List<FetchResponse>> future : futures) {
			allResponses.addAll(future.get());
		}
		return allResponses;
	}

	public ZuliaBase.AssociatedDocument getAssociatedDocument(String indexName, String uniqueId, String fileName) throws Exception {
		try (IndexLease lease = leaseIndexFromName(indexName)) {
			ZuliaIndex index = lease.getIndex();
			return index.getAssociatedDocument(uniqueId, fileName, ZuliaQuery.FetchType.FULL);
		}
	}

	public Document getAssociatedDocumentMeta(String indexName, String uniqueId, String fileName) throws Exception {
		try (IndexLease lease = leaseIndexFromName(indexName)) {
			ZuliaIndex index = lease.getIndex();
			ZuliaBase.AssociatedDocument associatedDocument = index.getAssociatedDocument(uniqueId, fileName, ZuliaQuery.FetchType.META);
			if (associatedDocument != null) {
				byte[] metadataBSONBytes = associatedDocument.getMetadata().toByteArray();
				return ZuliaUtil.byteArrayToMongoDocument(metadataBSONBytes);
			}
			return null;
		}
	}

	// the returned stream reads from document storage and outlives the lease, so eviction will need a stream-scoped hold
	public InputStream getAssociatedDocumentStream(String indexName, String uniqueId, String fileName) throws Exception {
		try (IndexLease lease = leaseIndexFromName(indexName)) {
			ZuliaIndex index = lease.getIndex();
			return index.getAssociatedDocumentStream(uniqueId, fileName);
		}
	}

	// the returned stream writes to document storage and outlives the lease, so eviction will need a stream-scoped hold
	public OutputStream getAssociatedDocumentOutputStream(String indexName, String uniqueId, String fileName, Document metadata) throws Exception {
		try (IndexLease lease = leaseIndexForWrite(indexName)) {
			ZuliaIndex i = lease.getIndex();
			long timestamp = System.currentTimeMillis();
			return i.getAssociatedDocumentOutputStream(uniqueId, fileName, timestamp, metadata);
		}
	}

	public List<String> getAssociatedFilenames(String indexName, String uniqueId) throws Exception {
		try (IndexLease lease = leaseIndexFromName(indexName)) {
			ZuliaIndex i = lease.getIndex();
			return i.getAssociatedFilenames(uniqueId);
		}
	}

	public Stream<AssociatedMetadataDTO> getAssociatedFilenames(String indexName, Document query) throws Exception {
		try (IndexLease lease = leaseIndexFromName(indexName)) {
			ZuliaIndex index = lease.getIndex();
			return index.getAssociatedMetadataForQuery(query);
		}
	}

	public GetNodesResponse getNodes(GetNodesRequest request) throws Exception {

		if ((request.getActiveOnly())) {
			Collection<RegisteredIndex> registeredIndexes = loadedIndexCache.getRegisteredIndexes();
			List<IndexShardMapping> indexShardMappingList = registeredIndexes.stream().map(RegisteredIndex::shardMapping).toList();
			List<IndexAlias> indexAliasesList = List.copyOf(indexAliasMap.values());
			return GetNodesResponse.newBuilder().addAllNode(currentOtherNodesActive).addNode(thisNode).addAllIndexShardMapping(indexShardMappingList)
					.addAllIndexAlias(indexAliasesList).build();
		}
		else {
			List<IndexShardMapping> indexShardMappingList = indexService.getIndexShardMappings();
			List<IndexAlias> indexAliasesList = indexService.getIndexAliases();
			return GetNodesResponse.newBuilder().addAllNode(nodeService.getNodes()).addAllIndexShardMapping(indexShardMappingList)
					.addAllIndexAlias(indexAliasesList).build();
		}
	}

	public InternalQueryResponse internalQuery(InternalQueryRequest request) throws Exception {

		try (IndexLeases leases = leaseQueryIndexes(request.getQueryRequest().getIndexList())) {
			Map<String, Query> queryMap = new HashMap<>();
			Set<ZuliaIndex> indexes = new HashSet<>();

			Map<String, ZuliaIndex> resolvedIndexes = leases.getIndexes();
			populateIndexesAndIndexMap(request.getQueryRequest(), resolvedIndexes, queryMap, indexes);

			return QueryRequestFederator.internalQuery(indexes, request, queryMap);
		}
	}

	public QueryResponse query(QueryRequest request) throws Exception {
		if (request.getIndexCount() == 0) {
			throw new IllegalArgumentException("Query requires at least one index");
		}

		request = new QueryRequestValidator().validateAndSetDefault(request);

		if (zuliaConfig.isDebug() && !request.getDebug()) {
			request = request.toBuilder().setDebug(true).build();
		}

		if (hasMLTQuery(request)) {
			// Validate MLT params and resolve document IDs into text + vectors before query building
			request = processMoreLikeThisQueries(request);
		}

		try (IndexLeases leases = leaseQueryIndexes(request.getIndexList())) {
			Map<String, Query> queryMap = new HashMap<>();
			Set<ZuliaIndex> indexes = new HashSet<>();

			Map<String, ZuliaIndex> resolvedIndexes = leases.getIndexes();
			populateIndexesAndIndexMap(request, resolvedIndexes, queryMap, indexes);

			QueryRequestFederator federator = new QueryRequestFederator(thisNode, currentOtherNodesActive, request.getPrimaryReplicaSettings(), indexes, pool,
					internalClient, queryMap);

			return federator.getResponse(request);
		}
	}

	private QueryRequest processMoreLikeThisQueries(QueryRequest request) throws Exception {
		List<ZuliaQuery.Query> queryList = request.getQueryList();

		try (IndexLeases queriedLeases = leaseQueryIndexes(request.getIndexList())) {
			Map<String, ZuliaIndex> queriedIndexes = queriedLeases.getIndexes();

			// For each MLT query: resolve its source indexes (its explicit sourceIndex list, or the queried indexes when none is given),
			// validate, fetch the source docs, and rewrite the query with the resolved text/vectors.
			QueryRequest.Builder requestBuilder = request.toBuilder().clearQuery();
			for (ZuliaQuery.Query q : queryList) {
				if (q.getQueryType() != ZuliaQuery.Query.QueryType.MORE_LIKE_THIS) {
					requestBuilder.addQuery(q);
					continue;
				}
				ZuliaQuery.MoreLikeThisParams mltParams = q.getMoreLikeThisParams();
				if (mltParams.getSourceIndexCount() == 0) {
					requestBuilder.addQuery(resolveMoreLikeThisQuery(q, queriedIndexes, queriedIndexes, request.getRealtime()));
				}
				else {
					try (IndexLeases sourceLeases = leaseQueryIndexes(mltParams.getSourceIndexList())) {
						Map<String, ZuliaIndex> indexMap = sourceLeases.getIndexes();
						ZuliaQuery.Query moreLikeThisQuery = resolveMoreLikeThisQuery(q, queriedIndexes, indexMap, request.getRealtime());
						requestBuilder.addQuery(moreLikeThisQuery);
					}
				}
			}

			return requestBuilder.build();
		}
	}

	private ZuliaQuery.Query resolveMoreLikeThisQuery(ZuliaQuery.Query q, Map<String, ZuliaIndex> queriedIndexes, Map<String, ZuliaIndex> sourceIndexes,
			boolean realtime) throws Exception {
		ZuliaQuery.MoreLikeThisParams mltParams = q.getMoreLikeThisParams();
		validateMoreLikeThisParams(mltParams, queriedIndexes, sourceIndexes);
		Map<String, Document> sourceDocs = fetchSourceDocs(mltParams, sourceIndexes, realtime);
		return rewriteMoreLikeThisQuery(q, queriedIndexes, sourceIndexes, sourceDocs);
	}

	/**
	 * Fetches each documentId's source document, walking the source indexes in priority order: each index is queried only for the
	 * document ids not yet resolved by a higher-priority index, so the first source index that has a document wins. Document ids
	 * not found in any source index are simply absent from the result (the rewrite reports them).
	 */
	private Map<String, Document> fetchSourceDocs(ZuliaQuery.MoreLikeThisParams mltParams, Map<String, ZuliaIndex> sourceIndexes, boolean realtime)
			throws Exception {
		Map<String, Document> sourceDocs = new HashMap<>();
		if (mltParams.getDocumentIdCount() == 0) {
			return sourceDocs;
		}

		Set<String> remaining = new LinkedHashSet<>(mltParams.getDocumentIdList());
		BatchFetchRequestFederator federator = new BatchFetchRequestFederator(thisNode, currentOtherNodesActive, pool, internalClient, sourceIndexes);

		for (String indexName : sourceIndexes.keySet()) {
			if (remaining.isEmpty()) {
				break;
			}
			BatchFetchRequest.Builder batchBuilder = BatchFetchRequest.newBuilder();
			for (String docId : remaining) {
				batchBuilder.addFetchRequest(
						FetchRequest.newBuilder().setUniqueId(docId).setIndexName(indexName).setResultFetchType(FetchType.FULL).setRealtime(realtime).build());
			}
			for (FetchResponse response : federator.send(batchBuilder.build())) {
				if (!response.hasResultDocument()) {
					continue;
				}
				ZuliaBase.ResultDocument rd = response.getResultDocument();
				if (rd.getDocument().isEmpty()) {
					continue;
				}
				if (remaining.remove(rd.getUniqueId())) {
					sourceDocs.put(rd.getUniqueId(), ResultHelper.getDocumentFromResultDocument(rd));
				}
			}
		}

		return sourceDocs;
	}

	private static boolean hasMLTQuery(QueryRequest queryRequest) {
		return queryRequest.getQueryList().stream().anyMatch(q -> q.getQueryType() == ZuliaQuery.Query.QueryType.MORE_LIKE_THIS);
	}

	private void validateMoreLikeThisParams(ZuliaQuery.MoreLikeThisParams mltParams, Map<String, ZuliaIndex> queriedIndexes,
			Map<String, ZuliaIndex> sourceIndexes) {
		String vectorField = mltParams.getVectorField();
		List<String> textFields = mltParams.getFieldList();
		boolean hasVectorField = !vectorField.isEmpty();

		if (textFields.isEmpty() && !hasVectorField) {
			throw new IllegalArgumentException("More-like-this query must specify text fields or a vector field");
		}

		if (mltParams.getDocumentIdCount() == 0 && mltParams.getLikeTextCount() == 0 && mltParams.getLikeVectorCount() == 0) {
			throw new IllegalArgumentException("More-like-this query must specify at least one source: document ids, like text, or like vectors");
		}

		if (textFields.isEmpty() && mltParams.getLikeTextCount() > 0) {
			throw new IllegalArgumentException("More-like-this query has like text but no text fields to analyze; specify text fields");
		}

		if (!hasVectorField && mltParams.getLikeVectorCount() > 0) {
			throw new IllegalArgumentException("More-like-this query has like vectors but no vector field; specify a vector field");
		}

		if (mltParams.getDocumentIdCount() > MLT_MAX_SOURCE_DOCS) {
			throw new IllegalArgumentException(
					"More-like-this query exceeds the source document limit: got " + mltParams.getDocumentIdCount() + ", max " + MLT_MAX_SOURCE_DOCS);
		}

		if (mltParams.getSourceIndexCount() > 0 && mltParams.getDocumentIdCount() == 0) {
			throw new IllegalArgumentException(
					"More-like-this sourceIndex only affects document id sources; specify at least one document id or remove sourceIndex");
		}

		// The generated query runs against the queried indexes, so the text/vector fields must exist there with searchable types,
		// and the vector field must be the same type across them (one centroid is built and one normalization decision is made).
		FieldConfig.FieldType expectedVectorType = null;
		String expectedVectorTypeIndex = null;
		for (Map.Entry<String, ZuliaIndex> entry : queriedIndexes.entrySet()) {
			String indexName = entry.getKey();
			ServerIndexConfig indexConfig = entry.getValue().getIndexConfig();
			validateMoreLikeThisFields(textFields, vectorField, hasVectorField, indexName, indexConfig, "index");
			if (hasVectorField) {
				FieldConfig.FieldType vectorType = indexConfig.getIndexFieldInfo(vectorField).getFieldType();
				if (expectedVectorType == null) {
					expectedVectorType = vectorType;
					expectedVectorTypeIndex = indexName;
				}
				else if (expectedVectorType != vectorType) {
					throw new IllegalArgumentException(
							"More-like-this vector field <" + vectorField + "> has inconsistent type across indexes: " + expectedVectorType + " in <"
									+ expectedVectorTypeIndex + ">, " + vectorType + " in <" + indexName + ">");
				}
			}
		}

		// When source docs come from explicit source indexes, the source fields must exist there so their text/vectors can be read
		if (mltParams.getSourceIndexCount() > 0) {
			for (Map.Entry<String, ZuliaIndex> entry : sourceIndexes.entrySet()) {
				validateMoreLikeThisFields(textFields, vectorField, hasVectorField, entry.getKey(), entry.getValue().getIndexConfig(), "source index");
			}
		}
	}

	/**
	 * Validates that each more-like-this text field exists as a STRING field and the vector field exists as a vector field in the
	 * given index. {@code role} labels the index in error messages (e.g. "index" or "source index").
	 */
	private static void validateMoreLikeThisFields(List<String> textFields, String vectorField, boolean hasVectorField, String indexName,
			ServerIndexConfig indexConfig, String role) {
		for (String field : textFields) {
			IndexFieldInfo info = indexConfig.getIndexFieldInfo(field);
			if (info == null) {
				throw new IllegalArgumentException("More-like-this text field <" + field + "> does not exist in " + role + " <" + indexName + ">");
			}
			if (!FieldTypeUtil.isStringFieldType(info.getFieldType())) {
				throw new IllegalArgumentException(
						"More-like-this text field <" + field + "> in " + role + " <" + indexName + "> must be a STRING field, got " + info.getFieldType());
			}
		}
		if (hasVectorField) {
			IndexFieldInfo info = indexConfig.getIndexFieldInfo(vectorField);
			if (info == null) {
				throw new IllegalArgumentException("More-like-this vector field <" + vectorField + "> does not exist in " + role + " <" + indexName + ">");
			}
			if (!FieldTypeUtil.isVectorFieldType(info.getFieldType())) {
				throw new IllegalArgumentException(
						"More-like-this vector field <" + vectorField + "> in " + role + " <" + indexName + "> must be VECTOR or UNIT_VECTOR, got "
								+ info.getFieldType());
			}
		}
	}

	private ZuliaQuery.Query rewriteMoreLikeThisQuery(ZuliaQuery.Query q, Map<String, ZuliaIndex> queriedIndexes, Map<String, ZuliaIndex> sourceIndexes,
			Map<String, Document> sourceDocs) {
		ZuliaQuery.MoreLikeThisParams mltParams = q.getMoreLikeThisParams();
		List<String> textFields = mltParams.getFieldList();
		String vectorField = mltParams.getVectorField();
		boolean hasVectorField = !vectorField.isEmpty();

		List<String> resolvedTexts = new ArrayList<>(mltParams.getLikeTextList());
		List<float[]> resolvedVectors = new ArrayList<>();
		List<String> vectorSources = new ArrayList<>();

		for (int i = 0; i < mltParams.getLikeVectorCount(); i++) {
			ZuliaQuery.VectorInput vi = mltParams.getLikeVector(i);
			float[] vec = new float[vi.getValuesCount()];
			for (int j = 0; j < vi.getValuesCount(); j++) {
				vec[j] = vi.getValues(j);
			}
			resolvedVectors.add(vec);
			vectorSources.add("user-supplied likeVector[" + i + "]");
		}

		for (String docId : mltParams.getDocumentIdList()) {
			Document bsonDoc = sourceDocs.get(docId);
			if (bsonDoc == null) {
				throw new IllegalArgumentException(
						"More-like-this source document <" + docId + "> not found in any of the source indexes " + sourceIndexes.keySet());
			}
			for (String field : textFields) {
				Object value = DocumentHelper.getValueFromMongoDocument(bsonDoc, field);
				if (value instanceof List<?> list) {
					for (Object item : list) {
						if (item != null) {
							resolvedTexts.add(item.toString());
						}
					}
				}
				else if (value != null) {
					resolvedTexts.add(value.toString());
				}
			}
			if (hasVectorField) {
				float[] vec = ZuliaUtil.getFloatArray(bsonDoc, vectorField, null);
				if (vec != null) {
					resolvedVectors.add(vec);
					vectorSources.add("doc <" + docId + ">");
				}
			}
		}

		boolean hasLexical = !textFields.isEmpty() && !resolvedTexts.isEmpty();
		boolean hasVector = hasVectorField && !resolvedVectors.isEmpty();
		if (!hasLexical && !hasVector) {
			List<String> missing = new ArrayList<>();
			if (!textFields.isEmpty()) {
				missing.add("text in fields " + textFields);
			}
			if (hasVectorField) {
				missing.add("vectors in field <" + vectorField + ">");
			}
			throw new IllegalArgumentException(
					"More-like-this source documents " + mltParams.getDocumentIdList() + " contained no " + String.join(" and no ", missing));
		}

		// documentId is kept so shards can exclude the source docs from results when includeSourceDocs is false
		ZuliaQuery.MoreLikeThisParams.Builder resolvedParams = mltParams.toBuilder().clearLikeText().clearLikeVector().clearResolvedVector();
		resolvedParams.addAllLikeText(resolvedTexts);

		// but when the source docs are fetched only from non-queried indexes, they can never appear in the queried results, so
		// keeping documentId would only suppress unrelated docs that happen to share an id. Drop it in that case.
		if (mltParams.getSourceIndexCount() > 0 && Collections.disjoint(sourceIndexes.keySet(), queriedIndexes.keySet())) {
			resolvedParams.clearDocumentId();
		}

		if (!resolvedVectors.isEmpty()) {
			int dim = resolvedVectors.getFirst().length;
			float[] centroid = new float[dim];
			for (int i = 0; i < resolvedVectors.size(); i++) {
				float[] vec = resolvedVectors.get(i);
				if (vec.length != dim) {
					throw new IllegalArgumentException(
							"More-like-this vector dimension mismatch: " + vectorSources.get(i) + " has " + vec.length + " dims, expected " + dim);
				}
				for (int j = 0; j < dim; j++) {
					centroid[j] += vec[j];
				}
			}
			for (int i = 0; i < dim; i++) {
				centroid[i] /= resolvedVectors.size();
			}

			boolean shouldNormalize = hasVectorField && queriedIndexes.values().iterator().next().getIndexConfig().getIndexFieldInfo(vectorField).getFieldType()
					== FieldConfig.FieldType.UNIT_VECTOR;
			if (shouldNormalize) {
				float norm = 0f;
				for (float v : centroid) {
					norm += v * v;
				}
				norm = (float) Math.sqrt(norm);
				if (norm > 0) {
					for (int i = 0; i < dim; i++) {
						centroid[i] /= norm;
					}
				}
			}

			for (float v : centroid) {
				resolvedParams.addResolvedVector(v);
			}
		}

		return q.toBuilder().setMoreLikeThisParams(resolvedParams).build();
	}

	private static void populateIndexesAndIndexMap(QueryRequest queryRequest, Map<String, ZuliaIndex> resolvedIndexes, Map<String, Query> queryMap,
			Set<ZuliaIndex> indexes) throws Exception {

		for (Map.Entry<String, ZuliaIndex> entry : resolvedIndexes.entrySet()) {
			ZuliaIndex index = entry.getValue();
			if (indexes.add(index)) {
				queryMap.put(entry.getKey(), index.getQuery(queryRequest));
			}
		}
	}

	/**
	 * Resolves the request's index names (expanding wildcard patterns and aliases) to a map of resolved index name to index, preserving request order.
	 */
	private Map<String, String> resolveQueryIndexes(List<String> indexNames) throws IndexDoesNotExistException {
		Map<String, String> resolved = new LinkedHashMap<>();
		for (String indexName : indexNames) {
			if (isWildcardPattern(indexName)) {
				List<String> matches = expandWildcard(indexName);
				if (matches.isEmpty()) {
					throw new IndexDoesNotExistException(indexName);
				}
				for (String matched : matches) {
					resolved.putIfAbsent(matched, matched);
				}
			}
			else {
				for (String resolvedName : resolveAlias(indexName)) {
					resolved.putIfAbsent(resolvedName, indexName);
				}
			}
		}
		return resolved;
	}

	/**
	 * Leases the request's indexes (expanding wildcard patterns and aliases), keyed by resolved index name, preserving request order.
	 * Indexes that are not resident are faulted in concurrently, bounded by the cache's load permits, so a
	 * wildcard query over many unloaded transient indexes does not pay serial cold loads.
	 */
	private IndexLeases leaseQueryIndexes(List<String> indexNames) throws Exception {
		Map<String, String> originalByResolved = resolveQueryIndexes(indexNames);

		Map<String, IndexLease> acquired = new HashMap<>();
		Map<String, Future<IndexLease>> loading = new HashMap<>();
		try {
			for (Map.Entry<String, String> entry : originalByResolved.entrySet()) {
				String resolvedName = entry.getKey();
				String originalName = entry.getValue();
				IndexLease lease = loadedIndexCache.tryLease(resolvedName);
				if (lease != null) {
					acquired.put(resolvedName, lease);
				}
				else {
					loading.put(resolvedName, pool.submit(() -> leaseIndex(originalName, resolvedName)));
				}
			}

			LinkedHashMap<String, IndexLease> leases = new LinkedHashMap<>();
			for (String resolvedName : originalByResolved.keySet()) {
				IndexLease lease = acquired.get(resolvedName);
				if (lease == null) {
					try {
						lease = loading.get(resolvedName).get();
					}
					catch (ExecutionException e) {
						throw e.getCause() instanceof Exception cause ? cause : e;
					}
					acquired.put(resolvedName, lease);
				}
				leases.put(resolvedName, lease);
			}
			return new IndexLeases(leases);
		}
		catch (Exception e) {
			// releasing every lease matters more than prompt interrupt response: a leaked lease pins its
			// index resident forever, so the joins here retry through interrupts and the flag is restored after
			boolean interrupted = Thread.interrupted();
			for (Future<IndexLease> future : loading.values()) {
				while (true) {
					try {
						IndexLease lease = future.get();
						if (lease != null) {
							lease.close();
						}
						break;
					}
					catch (InterruptedException duringCleanup) {
						interrupted = true;
					}
					catch (Exception ignored) {
						break;
					}
				}
			}
			// lease close is idempotent, so leases present in both maps close harmlessly twice
			acquired.values().forEach(IndexLease::close);
			if (interrupted) {
				Thread.currentThread().interrupt();
			}
			throw e;
		}
	}

	private static boolean isWildcardPattern(String indexName) {
		return indexName.indexOf('*') >= 0 || indexName.indexOf('?') >= 0;
	}

	private List<String> expandWildcard(String pattern) {
		Pattern regex = globToRegex(pattern);
		List<String> matches = new ArrayList<>();
		for (String existingName : loadedIndexCache.getRegisteredIndexNames()) {
			if (regex.matcher(existingName).matches()) {
				matches.add(existingName);
			}
		}
		return matches;
	}

	private static Pattern globToRegex(String glob) {
		StringBuilder sb = new StringBuilder(glob.length() + 4);
		sb.append('^');
		for (int i = 0; i < glob.length(); i++) {
			char c = glob.charAt(i);
			switch (c) {
				case '*' -> sb.append(".*");
				case '?' -> sb.append('.');
				case '.', '\\', '+', '(', ')', '[', ']', '{', '}', '^', '$', '|' -> sb.append('\\').append(c);
				default -> sb.append(c);
			}
		}
		sb.append('$');
		return Pattern.compile(sb.toString());
	}

	public StoreResponse store(StoreRequest request) throws Exception {
		try (IndexLease lease = leaseIndexForWrite(request.getIndexName())) {
			ZuliaIndex i = lease.getIndex();
			StoreRequestRouter router = new StoreRequestRouter(thisNode, currentOtherNodesActive, i, request.getUniqueId(), internalClient);
			return router.send(request);
		}
	}

	public StoreResponse internalStore(StoreRequest request) throws Exception {
		try (IndexLease lease = leaseIndexForWrite(request.getIndexName())) {
			ZuliaIndex i = lease.getIndex();
			return StoreRequestRouter.internalStore(i, request);
		}
	}

	public DeleteResponse delete(DeleteRequest request) throws Exception {
		try (IndexLease lease = leaseIndexForWrite(request.getIndexName())) {
			ZuliaIndex i = lease.getIndex();
			DeleteRequestRouter router = new DeleteRequestRouter(thisNode, currentOtherNodesActive, i, request.getUniqueId(), internalClient);
			return router.send(request);
		}
	}

	public DeleteResponse internalDelete(DeleteRequest request) throws Exception {
		try (IndexLease lease = leaseIndexForWrite(request.getIndexName())) {
			ZuliaIndex i = lease.getIndex();
			return DeleteRequestRouter.internalDelete(i, request);
		}
	}

	public void batchDelete(BatchDeleteRequest request, StreamObserver<DeleteResponse> responseObserver) throws Exception {
		if (request.getRequestList().isEmpty() && request.getBatchDeleteGroupList().isEmpty()) {
			return;
		}

		Set<String> indexNames = new HashSet<>();
		for (DeleteRequest deleteRequest : request.getRequestList()) {
			indexNames.add(deleteRequest.getIndexName());
		}
		for (BatchDeleteGroup group : request.getBatchDeleteGroupList()) {
			indexNames.add(group.getIndexName());
		}

		try (IndexLeases leases = IndexLeases.acquire(indexNames, this::leaseIndexForWrite)) {
			Map<String, ZuliaIndex> indexMap = leases.getIndexes();
			BatchDeleteRequestFederator federator = new BatchDeleteRequestFederator(thisNode, currentOtherNodesActive, pool, internalClient, indexMap);
			List<DeleteResponse> responses = federator.send(request);
			for (DeleteResponse response : responses) {
				responseObserver.onNext(response);
			}
		}
	}

	public List<DeleteResponse> internalBatchDelete(InternalBatchDeleteRequest request) throws Exception {
		List<InternalShardBatchDeleteRequest> shardRequests = request.getShardBatchDeleteRequestList();
		if (shardRequests.size() == 1) {
			try (IndexLease lease = leaseIndexForWrite(shardRequests.getFirst().getIndexName())) {
				ZuliaIndex index = lease.getIndex();
				return index.internalShardBatchDelete(shardRequests.getFirst());
			}
		}

		List<Future<List<DeleteResponse>>> futures = new ArrayList<>(shardRequests.size());
		for (InternalShardBatchDeleteRequest shardRequest : shardRequests) {
			futures.add(pool.submit(() -> {
				try (IndexLease lease = leaseIndexForWrite(shardRequest.getIndexName())) {
					ZuliaIndex index = lease.getIndex();
					return index.internalShardBatchDelete(shardRequest);
				}
			}));
		}

		List<DeleteResponse> allResponses = new ArrayList<>();
		for (Future<List<DeleteResponse>> future : futures) {
			allResponses.addAll(future.get());
		}
		return allResponses;
	}

	/**
	 * Resolves the per-field {@link FieldConfig#getDocValueSkipIndex()} flag before settings are persisted.
	 * <p>
	 * Any field that does not explicitly opt out defaults to on, whether it belongs to a brand-new index or is newly added
	 * to an existing one, so new fields get block-skipping range queries and dynamic-pruning sorts by default. For fields that
	 * already exist, the flag is frozen to whatever was persisted when it was first created.  Lucene treats
	 * the doc-values skip index as part of the immutable field schema. The IndexWriter throws if it changes, so we can't
	 * flip it on an existing field.
	 * <p>
	 */
	static IndexSettings applyDocValueSkipIndexPolicy(IndexSettings requested, IndexSettings existingIndex) {
		Map<String, Boolean> frozenByField = new HashMap<>();
		if (existingIndex != null) {
			for (FieldConfig fc : existingIndex.getFieldConfigList()) {
				frozenByField.put(fc.getStoredFieldName(), fc.getDocValueSkipIndex());
			}
		}

		IndexSettings.Builder builder = requested.toBuilder();
		for (FieldConfig.Builder fc : builder.getFieldConfigBuilderList()) {
			Boolean frozen = frozenByField.get(fc.getStoredFieldName());
			if (frozen != null) {
				// Existing field so keep the schema already on disk so the IndexWriter does not reject writes
				fc.setDocValueSkipIndex(frozen);
			}
			else if (!fc.hasDocValueSkipIndex()) {
				// New field (new index or added to an existing one) that did not opt out so default on.
				fc.setDocValueSkipIndex(true);
			}
		}
		return builder.build();
	}

	public CreateIndexResponse createIndex(CreateIndexRequest request) throws Exception {
		//if existing index make sure not to allow changing number of shards

		String requestString = getJsonString(request);

		LOG.info("{}:{} creating index: {}", zuliaConfig.getServerAddress(), zuliaConfig.getServicePort(), requestString);
		request = new CreateIndexRequestValidator().validateAndSetDefault(request);

		NodeWeightComputation nodeWeightComputation = new DefaultNodeWeightComputation(indexService, thisNode, currentOtherNodesActive);

		IndexSettings indexSettings = request.getIndexSettings();

		String indexName = indexSettings.getIndexName();

		// the same per-index lock updateIndex uses, so concurrent createIndex and updateIndex cannot
		// interleave their read-modify-write of settings. Held only for the local store but not the federation.
		Lock lock = loadedIndexCache.getUpdateLock(indexName);
		IndexSettings existingIndex;
		lock.lock();
		try {
			existingIndex = indexService.getIndex(indexName);

			indexSettings = applyDocValueSkipIndexPolicy(indexSettings, existingIndex);

			long currentTimeMillis = System.currentTimeMillis();
			if (existingIndex == null) {

				indexSettings = indexSettings.toBuilder().setCreateTime(currentTimeMillis).build();

				IndexShardMapping indexShardMapping = buildInitialShardMapping(indexSettings, nodeWeightComputation);
				indexService.storeIndexShardMapping(indexShardMapping);
			}
			else {

				IndexSettings existingComparable = existingIndex.toBuilder().clearCreateTime().clearUpdateTime().clearCreatedIndexVersion().build();
				IndexSettings requestComparable = indexSettings.toBuilder().clearCreateTime().clearUpdateTime().clearCreatedIndexVersion().build();
				if (existingComparable.equals(requestComparable)) {
					LOG.info("No changes to existing index {}", indexName);
					return CreateIndexResponse.newBuilder().build();
				}

				if (existingIndex.getNumberOfShards() != indexSettings.getNumberOfShards()) {
					throw new IllegalArgumentException("Cannot change shards for existing index");
				}

				if (existingIndex.getNumberOfReplicas() != indexSettings.getNumberOfReplicas()) {
					IndexShardMapping existingShardMapping = indexService.getIndexShardMapping(indexName);
					IndexShardMapping newShardMapping = adjustReplicaCount(existingShardMapping, indexSettings, nodeWeightComputation);
					indexService.storeIndexShardMapping(newShardMapping);
				}

			}

			IndexSettings.Builder timestampBuilder = indexSettings.toBuilder().setUpdateTime(currentTimeMillis);
			if (existingIndex != null) {
				timestampBuilder.setCreateTime(existingIndex.getCreateTime());
				// createdIndexVersion is stamped once at creation and frozen thereafter (server populated, like createTime).
				timestampBuilder.setCreatedIndexVersion(existingIndex.getCreatedIndexVersion());
			}
			else {
				timestampBuilder.setCreatedIndexVersion(ZuliaIndexVersion.CURRENT);
			}
			indexSettings = timestampBuilder.build();

			indexService.storeIndex(indexSettings);
		}
		finally {
			lock.unlock();
		}

		CreateOrUpdateIndexRequestFederator createOrUpdateIndexRequestFederator = new CreateOrUpdateIndexRequestFederator(thisNode, currentOtherNodesActive,
				pool, internalClient, this);

		try {
			@SuppressWarnings("unused") List<InternalCreateOrUpdateIndexResponse> send = createOrUpdateIndexRequestFederator.send(
					InternalCreateOrUpdateIndexRequest.newBuilder().setIndexName(indexName).build());
		}
		catch (Exception e) {
			if (existingIndex != null) {
				LOG.error("Failed to update index {}: ", request.getIndexSettings().getIndexName(), e);
				throw new Exception("Failed to update index " + request.getIndexSettings().getIndexName() + ": " + e.getMessage());
			}
			else {
				throw new Exception("Failed to create index " + request.getIndexSettings().getIndexName() + ": " + e.getMessage());

			}
		}

		return CreateIndexResponse.newBuilder().setChanged(true).build();
	}

	public UpdateIndexResponse updateIndex(UpdateIndexRequest request) throws Exception {

		String orgIndexName = request.getIndexName();

		if (orgIndexName.isEmpty()) {
			throw new IllegalArgumentException("Index name must be given");
		}

		String indexName = resolveWriteIndex(orgIndexName);

		if (!indexName.equals(orgIndexName)) {
			LOG.info("Update Index Following Alias {} to index {}", orgIndexName, indexName);
		}

		Lock lock = loadedIndexCache.getUpdateLock(indexName);
		IndexSettings indexSettings;
		lock.lock();
		try {
			indexSettings = indexService.getIndex(indexName);
			if (indexSettings == null) {
				LOG.error("Failed to update index {} that does not exist", indexName);

				throw new Exception("Failed to update index " + indexName + " that does not exist");
			}

			IndexSettings originalIndexSettings = indexSettings;

			String queryJson = getJsonString(request);
			LOG.info("Updating Index {} with {}", indexName, queryJson);

			UpdateIndexSettings updateIndexSettings = request.getUpdateIndexSettings();

			IndexSettings.Builder existingSettings = indexSettings.toBuilder();

			if (updateIndexSettings.getAnalyzerSettingsOperation().getEnable()) {
				List<AnalyzerSettings> analyzerSettings = updateWithAction(updateIndexSettings.getAnalyzerSettingsOperation(),
						existingSettings.getAnalyzerSettingsList(), updateIndexSettings.getAnalyzerSettingsList(), AnalyzerSettings::getName);
				existingSettings.clearAnalyzerSettings();
				existingSettings.addAllAnalyzerSettings(analyzerSettings);
			}

			if (updateIndexSettings.getFieldConfigOperation().getEnable()) {
				List<FieldConfig> fieldConfigs = updateWithAction(updateIndexSettings.getFieldConfigOperation(), existingSettings.getFieldConfigList(),
						updateIndexSettings.getFieldConfigList(), FieldConfig::getStoredFieldName);
				existingSettings.clearFieldConfig();
				existingSettings.addAllFieldConfig(fieldConfigs);
			}

			if (updateIndexSettings.getFieldMappingOperation().getEnable()) {
				List<FieldMapping> fieldMappingList = updateWithAction(updateIndexSettings.getFieldMappingOperation(), existingSettings.getFieldMappingList(),
						updateIndexSettings.getFieldMappingList(), FieldMapping::getAlias);
				existingSettings.clearFieldMapping();
				existingSettings.addAllFieldMapping(fieldMappingList);
			}

			if (updateIndexSettings.getWarmingSearchesOperation().getEnable()) {
				List<ByteString> existingWarmingBytes = existingSettings.getWarmingSearchesList();
				List<QueryRequest> existingWarmingSearch = new ArrayList<>();
				for (ByteString existingWarmingByte : existingWarmingBytes) {
					try {
						QueryRequest queryRequest = QueryRequest.parseFrom(existingWarmingByte);
						queryRequest = new QueryRequestValidator().validateAndSetDefault(queryRequest);
						existingWarmingSearch.add(queryRequest);
					}
					catch (Exception e) {
						LOG.error("Failed to parse existing warming search.  Removing from list: {}", e.getMessage());
					}
				}

				List<QueryRequest> warmingSearch = new ArrayList<>();
				for (ByteString updates : updateIndexSettings.getWarmingSearchesList()) {
					try {
						QueryRequest queryRequest = QueryRequest.parseFrom(updates);
						if (queryRequest.getSearchLabel().isEmpty()) {
							throw new RuntimeException("A search label is required for a warming search");
						}
						warmingSearch.add(queryRequest);
					}
					catch (Exception e) {
						throw new RuntimeException("Failed to parse existing warming search update", e);
					}
				}

				//TODO(Ian): Check if there was a change before resetting the warming (not compatible w/
				List<QueryRequest> warmingSearches = updateWithAction(updateIndexSettings.getWarmingSearchesOperation(), existingWarmingSearch, warmingSearch,
						QueryRequest::getSearchLabel);
				existingSettings.clearWarmingSearches();
				existingSettings.addAllWarmingSearches(warmingSearches.stream().map(QueryRequest::toByteString).toList());
			}

			if (updateIndexSettings.getSetDefaultSearchField()) {
				existingSettings.clearDefaultSearchField();
				existingSettings.addAllDefaultSearchField(updateIndexSettings.getDefaultSearchFieldList());
			}

			if (updateIndexSettings.getSetRequestFactor()) {
				existingSettings.setRequestFactor(updateIndexSettings.getRequestFactor());
			}

			if (updateIndexSettings.getSetMinShardRequest()) {
				existingSettings.setMinShardRequest(updateIndexSettings.getMinShardRequest());
			}

			if (updateIndexSettings.getSetShardTolerance()) {
				existingSettings.setShardTolerance(updateIndexSettings.getShardTolerance());
			}

			if (updateIndexSettings.getSetShardQueryCacheSize()) {
				existingSettings.setShardQueryCacheSize(updateIndexSettings.getShardQueryCacheSize());
			}

			if (updateIndexSettings.getSetShardQueryCacheMaxAmount()) {
				existingSettings.setShardQueryCacheMaxAmount(updateIndexSettings.getShardQueryCacheMaxAmount());
			}

			if (updateIndexSettings.getSetIdleTimeWithoutCommit()) {
				existingSettings.setIdleTimeWithoutCommit(updateIndexSettings.getIdleTimeWithoutCommit());
			}

			if (updateIndexSettings.getSetShardCommitInterval()) {
				existingSettings.setShardCommitInterval(updateIndexSettings.getShardCommitInterval());
			}

			if (updateIndexSettings.getSetRamBufferMB()) {
				existingSettings.setRamBufferMB(updateIndexSettings.getRamBufferMB());
			}

			if (updateIndexSettings.getSetMaxMergeThreads()) {
				existingSettings.setMaxMergeThreads(updateIndexSettings.getMaxMergeThreads());
			}

			if (updateIndexSettings.getSetMaxMergePending()) {
				existingSettings.setMaxMergePending(updateIndexSettings.getMaxMergePending());
			}

			if (updateIndexSettings.getSetIndexingThrottle()) {
				existingSettings.setIndexingThrottle(updateIndexSettings.getIndexingThrottle());
			}

			if (updateIndexSettings.getSetNrtIndexMaxMergeSizeMB()) {
				existingSettings.setNrtIndexMaxMergeSizeMB(updateIndexSettings.getNrtIndexMaxMergeSizeMB());
			}

			if (updateIndexSettings.getSetNrtIndexMaxCachedMB()) {
				existingSettings.setNrtIndexMaxCachedMB(updateIndexSettings.getNrtIndexMaxCachedMB());
			}

			if (updateIndexSettings.getSetNrtTaxoMaxMergeSizeMB()) {
				existingSettings.setNrtTaxoMaxMergeSizeMB(updateIndexSettings.getNrtTaxoMaxMergeSizeMB());
			}

			if (updateIndexSettings.getSetNrtTaxoMaxCachedMB()) {
				existingSettings.setNrtTaxoMaxCachedMB(updateIndexSettings.getNrtTaxoMaxCachedMB());
			}

			if (updateIndexSettings.getSetNrtCachingDisabled()) {
				existingSettings.setNrtCachingDisabled(updateIndexSettings.getNrtCachingDisabled());
			}

			if (updateIndexSettings.getSetIndexWeight()) {
				existingSettings.setIndexWeight(updateIndexSettings.getIndexWeight());
			}

			if (updateIndexSettings.getSetDisableCompression()) {
				existingSettings.setDisableCompression(updateIndexSettings.getDisableCompression());
			}

			if (updateIndexSettings.getSetDefaultConcurrency()) {
				existingSettings.setDefaultConcurrency(updateIndexSettings.getDefaultConcurrency());
			}

			if (updateIndexSettings.getSetDescription()) {
				existingSettings.setDescription(updateIndexSettings.getDescription());
			}

			if (updateIndexSettings.getSetNumberOfReplicas()) {
				existingSettings.setNumberOfReplicas(updateIndexSettings.getNumberOfReplicas());
			}

			if (updateIndexSettings.getSetTransientIndex()) {
				existingSettings.setTransientIndex(updateIndexSettings.getTransientIndex());
			}

			Operation metaUpdateOperation = updateIndexSettings.getMetaUpdateOperation();
			if (metaUpdateOperation.getEnable()) {
				Document existingMeta = ZuliaUtil.byteStringToMongoDocument(existingSettings.getMeta());

				Document newMetadata = ZuliaUtil.byteStringToMongoDocument(updateIndexSettings.getMetadata());

				if (OperationType.MERGE.equals(metaUpdateOperation.getOperationType())) {
					existingMeta.putAll(newMetadata);
				}
				else if (OperationType.REPLACE.equals(metaUpdateOperation.getOperationType())) {
					existingMeta = newMetadata;
				}
				else {
					throw new IllegalArgumentException("Unknown operation type " + metaUpdateOperation.getOperationType());
				}

				if (!metaUpdateOperation.getRemovedKeysList().isEmpty()) {
					for (String key : metaUpdateOperation.getRemovedKeysList()) {
						existingMeta.remove(key);
					}
				}

				existingSettings.setMeta(ZuliaUtil.mongoDocumentToByteString(existingMeta));
			}

			CreateIndexRequestValidator.validateIndexSettingsAndSetDefaults(existingSettings);
			// Freeze the doc-values skip index flag to the persisted schema for fields that already exist. An update must
			// never flip it on a live field (the IndexWriter would reject writes). New fields keep whatever was requested.
			indexSettings = applyDocValueSkipIndexPolicy(existingSettings.build(), originalIndexSettings);

			if (indexSettings.equals(originalIndexSettings)) {
				LOG.info("No changes to index {} from update", indexName);
				return UpdateIndexResponse.newBuilder().setFullIndexSettings(indexSettings).setChanged(false).build();
			}

			indexSettings = indexSettings.toBuilder().setUpdateTime(System.currentTimeMillis()).build();

			if (indexSettings.getNumberOfReplicas() != originalIndexSettings.getNumberOfReplicas()) {
				NodeWeightComputation nodeWeightComputation = new DefaultNodeWeightComputation(indexService, thisNode, currentOtherNodesActive);
				IndexShardMapping existingShardMapping = indexService.getIndexShardMapping(indexName);
				IndexShardMapping newShardMapping = adjustReplicaCount(existingShardMapping, indexSettings, nodeWeightComputation);
				indexService.storeIndexShardMapping(newShardMapping);
			}

			indexService.storeIndex(indexSettings);
		}
		finally {
			lock.unlock();
		}

		// Federate outside the per-index lock because the internal apply on this node may need the same lock to
		// fault in an unloaded index
		CreateOrUpdateIndexRequestFederator createOrUpdateIndexRequestFederator = new CreateOrUpdateIndexRequestFederator(thisNode, currentOtherNodesActive,
				pool, internalClient, this);

		try {
			@SuppressWarnings("unused") List<InternalCreateOrUpdateIndexResponse> send = createOrUpdateIndexRequestFederator.send(
					InternalCreateOrUpdateIndexRequest.newBuilder().setIndexName(indexName).build());

			return UpdateIndexResponse.newBuilder().setFullIndexSettings(indexSettings).setChanged(true).build();
		}
		catch (Exception e) {
			throw new Exception("Failed to update index " + indexName + ": " + e.getMessage());
		}
	}

	private static IndexShardMapping buildInitialShardMapping(IndexSettings indexSettings, NodeWeightComputation nodeWeightComputation) {
		IndexShardMapping.Builder indexShardMapping = IndexShardMapping.newBuilder();
		indexShardMapping.setIndexName(indexSettings.getIndexName());
		indexShardMapping.setNumberOfShards(indexSettings.getNumberOfShards());

		for (int i = 0; i < indexSettings.getNumberOfShards(); i++) {

			List<Node> nodes = nodeWeightComputation.getNodesSortedByWeight();

			ShardMapping.Builder shardMapping = ShardMapping.newBuilder().setShardNumber(i);

			Node primaryNode = nodes.removeFirst();
			shardMapping.setPrimaryNode(primaryNode);
			nodeWeightComputation.addShard(primaryNode, indexSettings, true);

			// remaining nodes are sorted by weight; take the least loaded ones as replicas
			nodes.stream().limit(indexSettings.getNumberOfReplicas()).forEach(replicaNode -> {
				shardMapping.addReplicaNode(replicaNode);
				nodeWeightComputation.addShard(replicaNode, indexSettings, false);
			});

			indexShardMapping.addShardMapping(shardMapping);
		}

		return indexShardMapping.build();
	}

	private static IndexShardMapping adjustReplicaCount(IndexShardMapping existing, IndexSettings indexSettings, NodeWeightComputation nodeWeightComputation) {
		int newReplicaCount = indexSettings.getNumberOfReplicas();

		IndexShardMapping.Builder builder = IndexShardMapping.newBuilder();
		builder.setIndexName(indexSettings.getIndexName());
		builder.setNumberOfShards(indexSettings.getNumberOfShards());

		for (ShardMapping currentShardMapping : existing.getShardMappingList()) {
			Node primaryNode = currentShardMapping.getPrimaryNode();
			List<Node> currentReplicas = currentShardMapping.getReplicaNodeList();

			ShardMapping.Builder shardBuilder = ShardMapping.newBuilder().setShardNumber(currentShardMapping.getShardNumber()).setPrimaryNode(primaryNode);

			if (newReplicaCount <= currentReplicas.size()) {
				// shrinking: keep the lowest-indexed replicas, drop the rest
				currentReplicas.stream().limit(newReplicaCount).forEach(shardBuilder::addReplicaNode);
			}
			else {
				// growing: keep all existing replicas and pick additional least-loaded nodes not already assigned to this shard
				currentReplicas.forEach(shardBuilder::addReplicaNode);

				List<Node> alreadyAssigned = Stream.concat(Stream.of(primaryNode), currentReplicas.stream()).toList();
				int toAdd = newReplicaCount - currentReplicas.size();

				nodeWeightComputation.getNodesSortedByWeight().stream()
						.filter(candidate -> alreadyAssigned.stream().noneMatch(assigned -> ZuliaNode.isEqual(assigned, candidate))).limit(toAdd)
						.forEach(replica -> {
							shardBuilder.addReplicaNode(replica);
							nodeWeightComputation.addShard(replica, indexSettings, false);
						});
			}

			builder.addShardMapping(shardBuilder);
		}

		return builder.build();
	}

	private static @NotNull String getJsonString(MessageOrBuilder request) throws InvalidProtocolBufferException {
		String queryJson = JsonFormat.printer().print(request);
		queryJson = queryJson.replace('\n', ' ').replaceAll("\\s+", " ");
		return queryJson;
	}

	private <T> List<T> updateWithAction(Operation operation, List<T> existingValues, List<T> updates, Function<T, String> keyFunction) {

		BinaryOperator<T> firstOne = (t, t2) -> t; // take the first one if duplicate labels

		List<T> newValues;
		if (OperationType.MERGE.equals(operation.getOperationType())) {
			if (!updates.isEmpty()) {

				Map<String, T> toUpdate = updates.stream().collect(Collectors.toMap(keyFunction, Function.identity(), firstOne, LinkedHashMap::new));

				List<T> existingWithReplacements = existingValues.stream().map(value -> {
					T replacement = toUpdate.get(keyFunction.apply(value));
					if (replacement != null) {
						toUpdate.remove(keyFunction.apply(value));
						return replacement;
					}
					return value;
				}).collect(Collectors.toList());
				existingWithReplacements.addAll(toUpdate.values());
				newValues = existingWithReplacements;
			}
			else {
				newValues = existingValues;
			}
		}
		else if (OperationType.REPLACE.equals(operation.getOperationType())) {
			newValues = new ArrayList<>(updates.stream().collect(Collectors.toMap(keyFunction, Function.identity(), firstOne, LinkedHashMap::new)).values());
		}
		else {
			throw new IllegalArgumentException("Unknown operation type " + operation.getOperationType());
		}

		if (!operation.getRemovedKeysList().isEmpty()) {
			Set<String> toDelete = new HashSet<>(operation.getRemovedKeysList());
			return newValues.stream().filter(value -> !toDelete.contains(keyFunction.apply(value))).collect(Collectors.toList());
		}
		return newValues;

	}

	public InternalCreateOrUpdateIndexResponse internalCreateOrUpdateIndex(String indexName) throws Exception {

		// the whole apply runs under the per-index lock: two federated applies can arrive concurrently
		// (updateIndex federates outside this lock), and applyShardMappingUpdate must never run twice at once
		Lock lock = loadedIndexCache.getUpdateLock(indexName);
		lock.lock();
		try {
			IndexLease lease = loadedIndexCache.tryLease(indexName);
			if (lease != null) {
				try (lease) {
					applyIndexUpdateToResident(lease.getIndex(), indexName);
					return InternalCreateOrUpdateIndexResponse.newBuilder().setLoaded(false).build();
				}
			}

			IndexShardMapping newShardMapping = indexService.getIndexShardMapping(indexName);
			RegisteredIndex registered = loadedIndexCache.getRegistered(indexName);
			if (registered == null) {
				// new index on this node: register and load. Transient indexes load here too so creation
				// verifies them, and the evictor prunes them later.
				loadedIndexCache.register(new RegisteredIndex(indexName, newShardMapping));
				try (IndexLease loadLease = loadedIndexCache.leaseOrLoad(indexName)) {
					// load only
				}
				return InternalCreateOrUpdateIndexResponse.newBuilder().setLoaded(true).build();
			}

			// not resident: settings apply at the next load, and only a role change for this node needs a
			// fault-in now (the load path reconciles the transitions from the previously applied mapping)
			if (checkIfLocalRolesChanged(registered.shardMapping(), newShardMapping)) {
				try (IndexLease loadLease = loadedIndexCache.leaseOrLoad(indexName)) {
					// fault in only, buildIndex applies the transitions
				}
			}
			else {
				loadedIndexCache.register(new RegisteredIndex(indexName, newShardMapping));
			}
			return InternalCreateOrUpdateIndexResponse.newBuilder().setLoaded(false).build();
		}
		finally {
			lock.unlock();
		}
	}

	private void applyIndexUpdateToResident(ZuliaIndex zuliaIndex, String indexName) throws Exception {
		IndexShardMapping newShardMapping = indexService.getIndexShardMapping(indexName);
		if (!zuliaIndex.getIndexShardMapping().equals(newShardMapping)) {
			LOG.info("{}:{} applying shard mapping update for index {}", zuliaConfig.getServerAddress(), zuliaConfig.getServicePort(), indexName);
			zuliaIndex.applyShardMappingUpdate(newShardMapping, node -> ZuliaNode.isEqual(thisNode, node));
		}

		zuliaIndex.reloadIndexSettings();
		loadedIndexCache.register(new RegisteredIndex(indexName, newShardMapping));
	}

	private record LocalShardRole(int shardNumber, boolean primary) {

	}

	private boolean checkIfLocalRolesChanged(IndexShardMapping oldMapping, IndexShardMapping newMapping) {
		return !localShardRoles(oldMapping).equals(localShardRoles(newMapping));
	}

	private Set<LocalShardRole> localShardRoles(IndexShardMapping mapping) {
		Set<LocalShardRole> roles = new HashSet<>();
		for (ShardMapping shardMapping : mapping.getShardMappingList()) {
			if (ZuliaNode.isEqual(shardMapping.getPrimaryNode(), thisNode)) {
				roles.add(new LocalShardRole(shardMapping.getShardNumber(), true));
			}
			else if (shardMapping.getReplicaNodeList().stream().anyMatch(node -> ZuliaNode.isEqual(node, thisNode))) {
				roles.add(new LocalShardRole(shardMapping.getShardNumber(), false));
			}
		}
		return roles;
	}

	public DeleteIndexResponse deleteIndex(DeleteIndexRequest request) throws Exception {
		LOG.info("{}:{} Received delete index request for {}", zuliaConfig.getServerAddress(), zuliaConfig.getServicePort(), request.getIndexName());

		DeleteIndexRequestFederator deleteIndexRequestFederator = new DeleteIndexRequestFederator(thisNode, currentOtherNodesActive, pool, internalClient,
				this);

		@SuppressWarnings("unused") List<DeleteIndexResponse> response = deleteIndexRequestFederator.send(request);

		String indexName = request.getIndexName();
		indexService.removeIndex(indexName);
		indexService.removeIndexShardMapping(indexName);

		return DeleteIndexResponse.newBuilder().build();

	}

	public DeleteIndexResponse internalDeleteIndex(DeleteIndexRequest request) throws Exception {
		String indexName = request.getIndexName();
		Lock lock = loadedIndexCache.getUpdateLock(indexName);
		lock.lock();
		try {
			if (!loadedIndexCache.isRegistered(indexName)) {
				LOG.info("{}:{} Index {} was not found", zuliaConfig.getServerAddress(), zuliaConfig.getServicePort(), request.getIndexName());
				return DeleteIndexResponse.newBuilder().build();
			}
			// fault in an unloaded transient index so its on-disk shard data and document storage are removed too
			try {
				try (IndexLease lease = loadedIndexCache.leaseOrLoad(indexName)) {
					if (lease != null) {
						LOG.info("{}:{} Deleting index {}", zuliaConfig.getServerAddress(), zuliaConfig.getServicePort(), request.getIndexName());
						lease.getIndex().deleteIndex(request.getDeleteAssociated());
						// forget the index while still holding the lease so the evictor can never drain the
						// handle in the gap between the teardown and the removal
						loadedIndexCache.removeDeleted(indexName);
						LOG.info("{}:{} Deleted index {}", zuliaConfig.getServerAddress(), zuliaConfig.getServicePort(), request.getIndexName());
					}
					else {
						loadedIndexCache.removeDeleted(indexName);
					}
				}
			}
			catch (Exception e) {
				// delete is the recovery path for an index that cannot even load, so remove its data directly
				LOG.warn("{}:{} Index {} could not be loaded for delete, removing its on-disk shard data directly", zuliaConfig.getServerAddress(),
						zuliaConfig.getServicePort(), indexName, e);
				deleteShardDataDirectlyFromDisk(indexName);
				loadedIndexCache.removeDeleted(indexName);
			}
		}
		finally {
			lock.unlock();
		}
		return DeleteIndexResponse.newBuilder().build();
	}

	private void deleteShardDataDirectlyFromDisk(String indexName) {
		RegisteredIndex registered = loadedIndexCache.getRegistered(indexName);
		int numberOfShards = registered != null && registered.shardMapping() != null ? registered.shardMapping().getNumberOfShards() : 0;
		for (int shardNumber = 0; shardNumber < numberOfShards; shardNumber++) {
			deleteDirectoryQuietly(ZuliaIndex.getPathForIndex(zuliaConfig, indexName, shardNumber));
			deleteDirectoryQuietly(ZuliaIndex.getPathForFacetsIndex(zuliaConfig, indexName, shardNumber));
		}
	}

	private static void deleteDirectoryQuietly(Path path) {
		try {
			if (Files.exists(path)) {
				Files.walkFileTree(path, new DeletingFileVisitor());
			}
		}
		catch (IOException e) {
			LOG.warn("Failed to delete {}", path, e);
		}
	}

	public GetNumberOfDocsResponse getNumberOfDocs(GetNumberOfDocsRequest request) throws Exception {
		try (IndexLease lease = leaseIndexFromName(request.getIndexName())) {
			ZuliaIndex index = lease.getIndex();
			GetNumberOfDocsRequestFederator federator = new GetNumberOfDocsRequestFederator(thisNode, currentOtherNodesActive,
					PrimaryReplicaSettings.PRIMARY_ONLY, index, pool, internalClient);
			return federator.getResponse(request);
		}
	}

	public GetNumberOfDocsResponse getNumberOfDocsInternal(InternalGetNumberOfDocsRequest internalRequest) throws Exception {
		try (IndexLease lease = leaseIndexFromName(internalRequest.getGetNumberOfDocsRequest().getIndexName())) {
			ZuliaIndex index = lease.getIndex();
			return GetNumberOfDocsRequestFederator.internalGetNumberOfDocs(index, internalRequest);
		}
	}

	public ClearResponse clear(ClearRequest request) throws Exception {
		try (IndexLease lease = leaseIndexForWrite(request.getIndexName())) {
			ZuliaIndex index = lease.getIndex();
			ClearRequestFederator federator = new ClearRequestFederator(thisNode, currentOtherNodesActive, PrimaryReplicaSettings.PRIMARY_ONLY, index, pool,
					internalClient);
			return federator.getResponse(request);
		}
	}

	public ClearResponse internalClear(ClearRequest request) throws Exception {
		try (IndexLease lease = leaseIndexForWrite(request.getIndexName())) {
			ZuliaIndex index = lease.getIndex();
			return ClearRequestFederator.internalClear(index, request);
		}
	}

	public OptimizeResponse optimize(OptimizeRequest request) throws Exception {
		try (IndexLease lease = leaseIndexForWrite(request.getIndexName())) {
			ZuliaIndex index = lease.getIndex();
			OptimizeRequestFederator federator = new OptimizeRequestFederator(thisNode, currentOtherNodesActive, PrimaryReplicaSettings.PRIMARY_ONLY, index,
					pool, internalClient);
			return federator.getResponse(request);
		}
	}

	public OptimizeResponse internalOptimize(OptimizeRequest request) throws Exception {
		try (IndexLease lease = leaseIndexForWrite(request.getIndexName())) {
			ZuliaIndex index = lease.getIndex();
			return OptimizeRequestFederator.internalOptimize(index, request);
		}
	}

	public ReindexResponse reindex(ReindexRequest request) throws Exception {
		try (IndexLease lease = leaseIndexForWrite(request.getIndexName())) {
			ZuliaIndex index = lease.getIndex();
			ReindexRequestFederator federator = new ReindexRequestFederator(thisNode, currentOtherNodesActive, PrimaryReplicaSettings.PRIMARY_ONLY, index, pool,
					internalClient);
			return federator.getResponse(request);
		}
	}

	public ReindexResponse internalReindex(ReindexRequest request) throws Exception {
		try (IndexLease lease = leaseIndexForWrite(request.getIndexName())) {
			ZuliaIndex index = lease.getIndex();
			return ReindexRequestFederator.internalReindex(index, request);
		}
	}

	public GetFieldNamesResponse getFieldNames(GetFieldNamesRequest request) throws Exception {
		PrimaryReplicaSettings primaryReplicaSettings = request.getPrimaryReplicaSettings();
		try (IndexLease lease = leaseIndexFromName(request.getIndexName())) {
			ZuliaIndex index = lease.getIndex();
			GetFieldNamesRequestFederator federator = new GetFieldNamesRequestFederator(thisNode, currentOtherNodesActive, primaryReplicaSettings, index, pool,
					internalClient);
			return federator.getResponse(request);
		}
	}

	public GetFieldNamesResponse internalGetFieldNames(InternalGetFieldNamesRequest request) throws Exception {
		GetFieldNamesRequest getFieldNamesRequest = request.getGetFieldNamesRequest();
		try (IndexLease lease = leaseIndexFromName(getFieldNamesRequest.getIndexName())) {
			ZuliaIndex index = lease.getIndex();
			return GetFieldNamesRequestFederator.internalGetFieldNames(index, request);
		}
	}

	public GetTermsResponse getTerms(GetTermsRequest request) throws Exception {
		try (IndexLease lease = leaseIndexFromName(request.getIndexName())) {
			ZuliaIndex index = lease.getIndex();
			GetTermsRequestFederator federator = new GetTermsRequestFederator(thisNode, currentOtherNodesActive, request.getPrimaryReplicaSettings(), index,
					pool, internalClient);
			return federator.getResponse(request);
		}
	}

	public InternalGetTermsResponse internalGetTerms(InternalGetTermsRequest request) throws Exception {
		GetTermsRequest getTermsRequest = request.getGetTermsRequest();
		try (IndexLease lease = leaseIndexFromName(getTermsRequest.getIndexName())) {
			ZuliaIndex index = lease.getIndex();
			return GetTermsRequestFederator.internalGetTerms(index, request);
		}
	}

	public GetIndexSettingsResponse getIndexSettings(GetIndexSettingsRequest request) throws Exception {
		String resolvedName = resolveWriteIndex(request.getIndexName());
		// must not load an unloaded transient index or refresh its recency so try quietly
		try (IndexLease lease = loadedIndexCache.tryLeaseQuietly(resolvedName)) {
			if (lease != null) {
				return GetIndexSettingsResponse.newBuilder().setIndexSettings(lease.getIndex().getIndexConfig().getIndexSettings()).build();
			}
		}
		IndexSettings indexSettings = indexService.getIndex(resolvedName);
		if (indexSettings == null) {
			if (resolvedName.equals(request.getIndexName())) {
				throw new IndexDoesNotExistException(resolvedName);
			}
			throw new IndexDoesNotExistException(request.getIndexName() + "->" + resolvedName);
		}
		return GetIndexSettingsResponse.newBuilder().setIndexSettings(indexSettings).build();
	}

	public List<ReplicationShardState> getReplicationState(String indexName) throws Exception {
		String resolvedName = resolveSingleIndex(indexName);
		// must not load an unloaded transient index or refresh its recency so try quietly
		try (IndexLease lease = loadedIndexCache.tryLeaseQuietly(resolvedName)) {
			if (lease != null) {
				return lease.getIndex().getReplicationState();
			}
		}
		if (!loadedIndexCache.isRegistered(resolvedName)) {
			throw new IndexDoesNotExistException(indexName);
		}
		return List.of();
	}

	public LoadedIndexCache getLoadedIndexCache() {
		return loadedIndexCache;
	}

	public IndexLease leaseIndexFromName(String indexName) throws Exception {
		return leaseIndex(indexName, resolveSingleIndex(indexName));
	}

	private IndexLease leaseIndexForWrite(String indexName) throws Exception {
		return leaseIndex(indexName, resolveWriteIndex(indexName));
	}

	private IndexLease leaseIndex(String originalName, String resolvedName) throws Exception {
		IndexLease lease = loadedIndexCache.leaseOrLoad(resolvedName);
		if (lease == null) {
			if (resolvedName.equals(originalName)) {
				throw new IndexDoesNotExistException(originalName);
			}
			throw new IndexDoesNotExistException(originalName + "->" + resolvedName);
		}
		return lease;
	}

	private List<String> resolveAlias(String indexName) {
		IndexAlias alias = indexAliasMap.get(indexName);
		return alias != null ? IndexAliasUtil.getIndexNames(alias) : List.of(indexName);
	}

	private String resolveSingleIndex(String indexName) {
		IndexAlias alias = indexAliasMap.get(indexName);
		return alias != null ? IndexAliasUtil.requireSingleIndex(alias) : indexName;
	}

	private String resolveWriteIndex(String indexName) {
		IndexAlias alias = indexAliasMap.get(indexName);
		return alias != null ? IndexAliasUtil.requireWriteIndex(alias) : indexName;
	}

	public CreateIndexAliasResponse createIndexAlias(CreateIndexAliasRequest request) throws Exception {
		IndexAlias indexAlias = request.getIndexAlias();

		String aliasName = indexAlias.getAliasName();

		if (aliasName.isEmpty()) {
			throw new IllegalArgumentException("Alias name cannot be null or empty");
		}

		List<String> indexNames = IndexAliasUtil.getIndexNames(indexAlias);
		if (indexNames.isEmpty()) {
			throw new IllegalArgumentException("At least one index name must be provided");
		}
		for (String name : indexNames) {
			if (name.isEmpty()) {
				throw new IllegalArgumentException("Index name cannot be null or empty");
			}
		}

		if (loadedIndexCache.isRegistered(aliasName)) {
			throw new IllegalArgumentException("Alias name <" + aliasName + "> collides with an existing index");
		}

		IndexAlias existingAlias = indexService.getIndexAlias(aliasName);

		indexService.storeIndexAlias(indexAlias);

		CreateIndexAliasRequestFederator createIndexRequestFederator = new CreateIndexAliasRequestFederator(thisNode, currentOtherNodesActive, pool,
				internalClient, this);

		try {
			@SuppressWarnings("unused") List<CreateIndexAliasResponse> send = createIndexRequestFederator.send(
					InternalCreateIndexAliasRequest.newBuilder().setAliasName(aliasName).build());
		}
		catch (Exception e) {
			if (existingAlias == null) {
				LOG.error("Failed to update index alias {}: ", aliasName, e);
				throw new Exception("Failed to update index alias " + aliasName + ": " + e.getMessage());
			}
			else {
				throw new Exception("Failed to create index alias " + aliasName + ": " + e.getMessage());

			}
		}

		return CreateIndexAliasResponse.newBuilder().build();

	}

	public DeleteIndexAliasResponse deleteIndexAlias(DeleteIndexAliasRequest request) throws Exception {
		indexService.removeIndexAlias(request.getAliasName());

		DeleteIndexAliasRequestFederator deleteIndexAliasRequestFederator = new DeleteIndexAliasRequestFederator(thisNode, currentOtherNodesActive, pool,
				internalClient, this);

		try {
			@SuppressWarnings("unused") List<DeleteIndexAliasResponse> send = deleteIndexAliasRequestFederator.send(request);
		}
		catch (Exception e) {
			throw new Exception("Failed to delete index alias " + request.getAliasName() + ": " + e.getMessage());
		}

		return DeleteIndexAliasResponse.newBuilder().build();

	}

	public CreateIndexAliasResponse internalCreateIndexAlias(String aliasName) throws Exception {
		IndexAlias indexAlias = indexService.getIndexAlias(aliasName);
		indexAliasMap.put(indexAlias.getAliasName(), indexAlias);
		return CreateIndexAliasResponse.newBuilder().build();
	}

	public DeleteIndexAliasResponse internalDeleteIndexAlias(DeleteIndexAliasRequest request) {
		indexAliasMap.remove(request.getAliasName());
		return DeleteIndexAliasResponse.newBuilder().build();
	}

	public List<IndexStats> getIndexStats() throws IOException {
		// every registered index is reported, and an unloaded transient index is a bare non-resident entry
		List<IndexStats> list = new ArrayList<>();
		for (String indexName : loadedIndexCache.getRegisteredIndexNames().stream().sorted().toList()) {
			try (IndexLease lease = loadedIndexCache.tryLeaseQuietly(indexName)) {
				if (lease != null) {
					list.add(lease.getIndex().getIndexStats());
				}
				else {
					list.add(IndexStats.newBuilder().setIndexName(indexName).setResident(false).build());
				}
			}
		}
		return list;
	}
}
