package io.zulia.server.index;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.MessageOrBuilder;
import com.google.protobuf.util.JsonFormat;
import io.zulia.message.ZuliaBase;
import io.zulia.message.ZuliaBase.MasterSlaveSettings;
import io.zulia.message.ZuliaBase.Node;
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
import io.zulia.message.ZuliaServiceOuterClass.*;
import io.zulia.rest.dto.AssociatedMetadataDTO;
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
import io.zulia.server.filestorage.DocumentStorage;
import io.zulia.server.filestorage.FileDocumentStorage;
import io.zulia.server.filestorage.MongoDocumentStorage;
import io.zulia.server.filestorage.S3DocumentStorage;
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
import io.zulia.server.index.router.DeleteRequestRouter;
import io.zulia.server.index.router.FetchRequestRouter;
import io.zulia.server.index.router.StoreRequestRouter;
import io.zulia.server.node.ZuliaNode;
import io.zulia.server.util.MongoProvider;
import io.zulia.util.ZuliaThreadFactory;
import io.zulia.util.ZuliaUtil;
import org.apache.lucene.search.Query;
import org.bson.Document;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ZuliaIndexManager {

	private final static Logger LOG = LoggerFactory.getLogger(ZuliaIndexManager.class);
	private final IndexService indexService;
	private final InternalClient internalClient;
	private final ExecutorService pool;
	private final ConcurrentHashMap<String, ZuliaIndex> indexMap;
	private final ZuliaConfig zuliaConfig;
	private final NodeService nodeService;
	private final Node thisNode;
	private Collection<Node> currentOtherNodesActive = Collections.emptyList();
	private ConcurrentHashMap<String, Lock> indexUpdateMap = new ConcurrentHashMap<>();
	private final ConcurrentHashMap<String, String> indexAliasMap;

	public ZuliaIndexManager(ZuliaConfig zuliaConfig, NodeService nodeService) throws Exception {

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
		this.indexAliasMap = new ConcurrentHashMap<>();

		for (IndexAlias indexAlias : indexService.getIndexAliases()) {
			indexAliasMap.put(indexAlias.getAliasName(), indexAlias.getIndexName());
		}

		this.pool = Executors.newCachedThreadPool(new ZuliaThreadFactory("manager"));

	}

	public void handleNodeAdded(Collection<Node> currentOtherNodesActive, Node nodeAdded) {
		LOG.info("{}:{} added node {}:{}", zuliaConfig.getServerAddress(), zuliaConfig.getServicePort(), nodeAdded.getServerAddress(), nodeAdded.getServicePort());

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

		indexMap.values().parallelStream().forEach(zuliaIndex -> {
			try {
				zuliaIndex.unload(false);
			}
			catch (IOException e) {
				LOG.error("Failed to unload index: {}", zuliaIndex.getIndexName(), e);
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
					LOG.error("{}:", e.getClass().getSimpleName(), e);
					throw new RuntimeException(e);
				}
			});
		}

	}

	private void loadIndex(String indexName) throws Exception {

		IndexSettings indexSettings = indexService.getIndex(indexName);

		if (indexSettings == null) {
			throw new IndexDoesNotExistException(indexName);
		}

		loadIndex(indexSettings);
	}

	private void loadIndex(IndexSettings indexSettings) throws Exception {
		LOG.info("{}:{} loading index {}", zuliaConfig.getServerAddress(), zuliaConfig.getServicePort(), indexSettings.getIndexName());

		IndexShardMapping indexShardMapping = indexService.getIndexShardMapping(indexSettings.getIndexName());

		ServerIndexConfig serverIndexConfig = new ServerIndexConfig(indexSettings);

		DocumentStorage documentStorage = getDocumentStorage(serverIndexConfig);

		ZuliaIndex zuliaIndex = new ZuliaIndex(zuliaConfig, serverIndexConfig, documentStorage, indexService, indexShardMapping);

		indexMap.put(indexSettings.getIndexName(), zuliaIndex);

		zuliaIndex.loadShards((node) -> ZuliaNode.isEqual(thisNode, node));
	}

	@NotNull
	private DocumentStorage getDocumentStorage(ServerIndexConfig serverIndexConfig) {
		String dbName = zuliaConfig.getClusterName() + "_" + serverIndexConfig.getIndexName() + "_" + "fs";

		DocumentStorage documentStorage;
		if (zuliaConfig.isCluster()) {
			documentStorage = switch (zuliaConfig.getClusterStorageEngine()) {
				case "s3" -> new S3DocumentStorage(MongoProvider.getMongoClient(), serverIndexConfig.getIndexName(), dbName, false, zuliaConfig.getS3());
				default -> new MongoDocumentStorage(MongoProvider.getMongoClient(), serverIndexConfig.getIndexName(), dbName, false);
			};
			;
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

	public Document getAssociatedDocumentMeta(String indexName, String uniqueId, String fileName) throws Exception {
		ZuliaIndex i = getIndexFromName(indexName);
		ZuliaBase.AssociatedDocument associatedDocument = i.getAssociatedDocument(uniqueId, fileName, ZuliaQuery.FetchType.META);
		if (associatedDocument != null) {
			byte[] metadataBSONBytes = associatedDocument.getMetadata().toByteArray();
			return ZuliaUtil.byteArrayToMongoDocument(metadataBSONBytes);
		}
		return null;
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

	public Stream<AssociatedMetadataDTO> getAssociatedFilenames(String indexName, Document query) throws Exception {
		ZuliaIndex i = getIndexFromName(indexName);
		return i.getAssociatedMetadataForQuery(query);
	}

	public GetNodesResponse getNodes(GetNodesRequest request) throws Exception {

		List<IndexShardMapping> indexShardMappingList = indexService.getIndexShardMappings();
		List<IndexAlias> indexAliasesList = indexService.getIndexAliases();
		if ((request.getActiveOnly())) {
			return GetNodesResponse.newBuilder().addAllNode(currentOtherNodesActive).addNode(thisNode).addAllIndexShardMapping(indexShardMappingList)
					.addAllIndexAlias(indexAliasesList).build();
		}
		else {
			return GetNodesResponse.newBuilder().addAllNode(nodeService.getNodes()).addAllIndexShardMapping(indexShardMappingList)
					.addAllIndexAlias(indexAliasesList).build();
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
			queryMap.put(handleAlias(indexName), query);
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

		String requestString = getJsonString(request);

		LOG.info("{}:{} creating index: {}", zuliaConfig.getServerAddress(), zuliaConfig.getServicePort(), requestString);
		request = new CreateIndexRequestValidator().validateAndSetDefault(request);

		NodeWeightComputation nodeWeightComputation = new DefaultNodeWeightComputation(indexService, thisNode, currentOtherNodesActive);

		IndexSettings indexSettings = request.getIndexSettings();

		String indexName = indexSettings.getIndexName();
		IndexSettings existingIndex = indexService.getIndex(indexName);

		long currentTimeMillis = System.currentTimeMillis();
		if (existingIndex == null) {

			indexSettings = indexSettings.toBuilder().setCreateTime(currentTimeMillis).build();

			IndexShardMapping.Builder indexShardMapping = IndexShardMapping.newBuilder();
			indexShardMapping.setIndexName(indexName);
			indexShardMapping.setNumberOfShards(indexSettings.getNumberOfShards());

			for (int i = 0; i < indexSettings.getNumberOfShards(); i++) {

				List<Node> nodes = nodeWeightComputation.getNodesSortedByWeight();

				ShardMapping.Builder shardMapping = ShardMapping.newBuilder();

				Node primaryNode = nodes.removeFirst();
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

				indexShardMapping.addShardMapping(shardMapping);
			}

			indexService.storeIndexShardMapping(indexShardMapping.build());
		}
		else {

			if (existingIndex.equals(indexSettings)) {
				LOG.info("No changes to existing index {}", indexName);
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
		indexService.storeIndex(indexSettings);

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

		return CreateIndexResponse.newBuilder().build();
	}

	public UpdateIndexResponse updateIndex(UpdateIndexRequest request) throws Exception {

		String orgIndexName = request.getIndexName();

		if (orgIndexName.isEmpty()) {
			throw new IllegalArgumentException("Index name must be given");
		}

		String indexName = handleAlias(orgIndexName);

		if (!indexName.equals(orgIndexName)) {
			LOG.info("Update Index Following Alias {} to index {}", orgIndexName, indexName);
		}

		Lock lock = indexUpdateMap.computeIfAbsent(indexName, s -> new ReentrantLock());
		try {
			lock.lock();

			IndexSettings indexSettings = indexService.getIndex(indexName);
			if (indexSettings == null) {
				LOG.error("Failed to update index {} that does not exist", indexName);

				throw new Exception("Failed to update index " + indexName + " that does not exist");
			}

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

			if (updateIndexSettings.getSetIndexWeight()) {
				existingSettings.setIndexWeight(updateIndexSettings.getIndexWeight());
			}

			if (updateIndexSettings.getSetDisableCompression()) {
				existingSettings.setDisableCompression(updateIndexSettings.getDisableCompression());
			}

			if (updateIndexSettings.getSetDefaultConcurrency()) {
				existingSettings.setDefaultConcurrency(updateIndexSettings.getDefaultConcurrency());
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
			indexSettings = existingSettings.build();

			indexService.storeIndex(indexSettings);

			CreateOrUpdateIndexRequestFederator createOrUpdateIndexRequestFederator = new CreateOrUpdateIndexRequestFederator(thisNode, currentOtherNodesActive,
					pool, internalClient, this);

			try {
				@SuppressWarnings("unused") List<InternalCreateOrUpdateIndexResponse> send = createOrUpdateIndexRequestFederator.send(
						InternalCreateOrUpdateIndexRequest.newBuilder().setIndexName(indexName).build());

				return UpdateIndexResponse.newBuilder().setFullIndexSettings(indexSettings).build();
			}
			catch (Exception e) {
				throw new Exception("Failed to update index " + indexName + ": " + e.getMessage());
			}
		}
		finally {
			lock.unlock();
		}
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

		ZuliaIndex zuliaIndex = indexMap.get(indexName);
		if (zuliaIndex == null) {
			loadIndex(indexName);
		}
		else {
			zuliaIndex.reloadIndexSettings();
		}

		return InternalCreateOrUpdateIndexResponse.newBuilder().setLoaded(zuliaIndex == null).build();

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
		ZuliaIndex zuliaIndex = indexMap.get(indexName);
		if (zuliaIndex != null) {
			LOG.info("{}:{} Deleting index {}", zuliaConfig.getServerAddress(), zuliaConfig.getServicePort(), request.getIndexName());
			//zuliaIndex.unload(true);
			zuliaIndex.deleteIndex(request.getDeleteAssociated());
			indexMap.remove(indexName);
			LOG.info("{}:{} Deleted index {}", zuliaConfig.getServerAddress(), zuliaConfig.getServicePort(), request.getIndexName());
		}
		else {
			LOG.info("{}:{} Index {} was not found", zuliaConfig.getServerAddress(), zuliaConfig.getServicePort(), request.getIndexName());
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
		if (i != null) {
			return GetIndexSettingsResponse.newBuilder().setIndexSettings(i.getIndexConfig().getIndexSettings()).build();
		}
		return null;
	}

	private ZuliaIndex getIndexFromName(String indexName) throws IndexDoesNotExistException {

		String orgIndex = indexName;
		indexName = handleAlias(indexName);

		ZuliaIndex i = indexMap.get(indexName);
		if (i == null) {

			if (orgIndex.equals(indexName)) {
				throw new IndexDoesNotExistException(indexName);
			}
			else {
				throw new IndexDoesNotExistException(orgIndex + "->" + indexName);
			}
		}
		return i;
	}

	private String handleAlias(String indexName) {
		if (indexAliasMap.containsKey(indexName)) {
			indexName = indexAliasMap.get(indexName);
		}
		return indexName;
	}

	public CreateIndexAliasResponse createIndexAlias(CreateIndexAliasRequest request) throws Exception {
		IndexAlias indexAlias = request.getIndexAlias();

		String aliasName = indexAlias.getAliasName();

		if (aliasName.isEmpty()) {
			throw new IllegalArgumentException("Alias name cannot be null or empty");
		}

		if (indexAlias.getIndexName().isEmpty()) {
			throw new IllegalArgumentException("Index name cannot be null or empty");
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
		indexAliasMap.put(indexAlias.getAliasName(), indexAlias.getIndexName());
		return CreateIndexAliasResponse.newBuilder().build();
	}

	public DeleteIndexAliasResponse internalDeleteIndexAlias(DeleteIndexAliasRequest request) {
		indexAliasMap.remove(request.getAliasName());
		return DeleteIndexAliasResponse.newBuilder().build();
	}

	public void getStats() {
		for (ZuliaIndex value : indexMap.values()) {

		}
	}
}
