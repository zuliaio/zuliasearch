package io.zulia.server.index;

import io.zulia.message.ZuliaBase;
import io.zulia.message.ZuliaBase.AssociatedDocument;
import io.zulia.message.ZuliaBase.Node;
import io.zulia.message.ZuliaBase.Term;
import io.zulia.message.ZuliaIndex;
import io.zulia.message.ZuliaQuery.FetchType;
import io.zulia.message.ZuliaServiceOuterClass.*;
import io.zulia.server.config.IndexService;
import io.zulia.server.config.ServerIndexConfig;
import io.zulia.server.config.ZuliaConfig;
import io.zulia.server.config.cluster.MongoIndexService;
import io.zulia.server.config.single.FSIndexService;
import io.zulia.server.connection.client.InternalClient;
import io.zulia.server.exceptions.IndexDoesNotExist;
import io.zulia.server.filestorage.DocumentStorage;
import io.zulia.server.filestorage.MongoDocumentStorage;
import io.zulia.server.node.ZuliaNode;
import io.zulia.server.util.MongoProvider;
import io.zulia.util.ZuliaThreadFactory;
import org.bson.Document;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

public class ZuliaIndexManager {

	private static final Logger LOG = Logger.getLogger(ZuliaIndexManager.class.getName());

	private final IndexService indexService;
	private final InternalClient internalClient;
	private final ExecutorService pool;
	private final ConcurrentHashMap<String, io.zulia.server.index.ZuliaIndex> indexMap;

	private final ZuliaConfig zuliaConfig;

	private Node thisNode;
	private Collection<Node> currentOtherNodesActive;

	public ZuliaIndexManager(ZuliaConfig zuliaConfig) throws Exception {

		this.zuliaConfig = zuliaConfig;

		this.thisNode = ZuliaNode.nodeFromConfig(zuliaConfig);

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
		System.out.println("nodeAdded: " + nodeAdded.getServerAddress() + ":" + nodeAdded.getServicePort());

		internalClient.addNode(nodeAdded);
		this.currentOtherNodesActive = currentOtherNodesActive;
	}

	public void handleNodeRemoved(Collection<Node> currentOtherNodesActive, Node nodeRemoved) {
		System.out.println("nodeRemoved: " + nodeRemoved.getServerAddress() + ":" + nodeRemoved.getServicePort());
		internalClient.removeNode(nodeRemoved);
	}

	public void shutdown() {

	}

	public void loadIndexes() throws Exception {
		List<ZuliaIndex.IndexSettings> indexes = indexService.getIndexes();
		for (ZuliaIndex.IndexSettings indexSettings : indexes) {

			loadIndex(indexSettings);

		}
	}

	private void loadIndex(ZuliaIndex.IndexSettings indexSettings) throws Exception {
		ZuliaIndex.IndexMapping indexMapping = indexService.getIndexMapping(indexSettings.getIndexName());

		pool.submit(() -> {

			ServerIndexConfig serverIndexConfig = new ServerIndexConfig(indexSettings);

			String dbName = zuliaConfig.getClusterName() + "_" + serverIndexConfig.getIndexName() + "_" + "fs";

			//TODO switch this on cluster moder / single
			DocumentStorage documentStorage = new MongoDocumentStorage(MongoProvider.getMongoClient(), serverIndexConfig.getIndexName(), dbName, false);

			io.zulia.server.index.ZuliaIndex zuliaIndex = new io.zulia.server.index.ZuliaIndex(serverIndexConfig, documentStorage, indexService);
			zuliaIndex.setIndexMapping(indexMapping);

			indexMap.put(indexSettings.getIndexName(), zuliaIndex);

			zuliaIndex.loadShards();
		});
	}

	public void openConnections(Collection<Node> nodes) {
		for (Node node : nodes) {
			internalClient.addNode(node);
		}
	}

	public GetIndexesResponse getIndexes(GetIndexesRequest request) throws Exception {
		GetIndexesResponse.Builder getIndexesResponse = GetIndexesResponse.newBuilder();
		for (ZuliaIndex.IndexSettings indexSettings : indexService.getIndexes()) {
			getIndexesResponse.addIndexName(indexSettings.getIndexName());
		}
		return getIndexesResponse.build();
	}

	public FetchResponse fetch(FetchRequest request) throws Exception {
		io.zulia.server.index.ZuliaIndex i = getIndexFromName(request.getIndexName());

		RequestNodeRouter<FetchRequest, FetchResponse> router = new RequestNodeRouter<FetchRequest, FetchResponse>(thisNode, currentOtherNodesActive,
				request.getMasterSlaveSettings(), i, request.getUniqueId()) {
			@Override
			protected FetchResponse processExternal(Node node, FetchRequest request) throws Exception {
				return internalClient.executeFetch(node, request);
			}

			@Override
			protected FetchResponse processInternal(FetchRequest request) throws Exception {
				return internalFetch(request);
			}
		};
		return router.send(request);

	}

	public FetchResponse internalFetch(FetchRequest fetchRequest) throws Exception {

		io.zulia.server.index.ZuliaIndex i = getIndexFromName(fetchRequest.getIndexName());
		FetchResponse.Builder frBuilder = FetchResponse.newBuilder();

		String uniqueId = fetchRequest.getUniqueId();

		FetchType resultFetchType = fetchRequest.getResultFetchType();
		if (!FetchType.NONE.equals(resultFetchType)) {

			ZuliaBase.ResultDocument resultDoc = i
					.getSourceDocument(uniqueId, resultFetchType, fetchRequest.getDocumentFieldsList(), fetchRequest.getDocumentMaskedFieldsList());
			if (null != resultDoc) {
				frBuilder.setResultDocument(resultDoc);
			}
		}

		FetchType associatedFetchType = fetchRequest.getAssociatedFetchType();
		if (!FetchType.NONE.equals(associatedFetchType)) {
			if (fetchRequest.getFilename() != null) {
				AssociatedDocument ad = i.getAssociatedDocument(uniqueId, fetchRequest.getFilename(), associatedFetchType);
				if (ad != null) {
					frBuilder.addAssociatedDocument(ad);
				}
			}
			else {
				for (AssociatedDocument ad : i.getAssociatedDocuments(uniqueId, associatedFetchType)) {
					frBuilder.addAssociatedDocument(ad);
				}
			}
		}
		return frBuilder.build();

	}

	public InputStream getAssociatedDocumentStream(String indexName, String uniqueId, String fileName) {
		return null;
	}

	public void storeAssociatedDocument(String indexName, String uniqueId, String fileName, InputStream is, Boolean compressed,
			HashMap<String, String> metaMap) {

	}

	public void getAssociatedDocuments(String indexName, OutputStream output, Document filter) {

	}

	public GetNodesResponse getNodes(GetNodesRequest request) {
		return null;
	}

	public InternalQueryResponse internalQuery(QueryRequest request) {

		return null;
	}

	public QueryResponse query(QueryRequest request) {
		return null;
	}

	public StoreResponse store(StoreRequest request) {
		return null;
	}

	public DeleteResponse delete(DeleteRequest request) {
		return null;
	}

	public BatchDeleteResponse batchDelete(BatchDeleteRequest request) {
		return null;
	}

	public BatchFetchResponse batchFetch(BatchFetchRequest request) {
		return null;
	}

	public CreateIndexResponse createIndex(CreateIndexRequest request) {
		return null;
	}

	public DeleteIndexResponse deleteIndex(DeleteIndexRequest request) {
		return null;
	}

	public GetNumberOfDocsResponse getNumberOfDocs(GetNumberOfDocsRequest request) {
		return null;
	}

	public GetNumberOfDocsResponse getNumberOfDocsInternal(GetNumberOfDocsRequest request) {
		return null;
	}

	public ClearResponse clear(ClearRequest request) {
		return null;
	}

	public ClearResponse internalClear(ClearRequest request) {
		return null;
	}

	public OptimizeResponse optimize(OptimizeRequest request) {
		return null;
	}

	public OptimizeResponse internalOptimize(OptimizeRequest request) throws Exception {
		String indexName = request.getIndexName();

		io.zulia.server.index.ZuliaIndex i = getIndexFromName(indexName);
		i.optimize();
		return OptimizeResponse.newBuilder().build();
	}

	public GetFieldNamesResponse getFieldNames(GetFieldNamesRequest request) throws Exception {
		String indexName = request.getIndexName();
		ZuliaBase.MasterSlaveSettings masterSlaveSettings = request.getMasterSlaveSettings();

		io.zulia.server.index.ZuliaIndex i = getIndexFromName(indexName);

		RequestNodeFederator<GetFieldNamesRequest, GetFieldNamesResponse> federator = new RequestNodeFederator<GetFieldNamesRequest, GetFieldNamesResponse>(
				thisNode, currentOtherNodesActive, masterSlaveSettings, i, pool) {

			@Override
			public GetFieldNamesResponse processExternal(Node node, GetFieldNamesRequest request) throws Exception {
				return internalClient.getFieldNames(node, request);
			}

			@Override
			public GetFieldNamesResponse processInternal(GetFieldNamesRequest request) throws Exception {
				return internalGetFieldNames(request);
			}

		};

		Set<String> fieldNames = new HashSet<>();
		List<GetFieldNamesResponse> responses = federator.send(request);
		for (GetFieldNamesResponse response : responses) {
			fieldNames.addAll(response.getFieldNameList());
		}

		GetFieldNamesResponse.Builder responseBuilder = GetFieldNamesResponse.newBuilder();
		responseBuilder.addAllFieldName(fieldNames);
		return responseBuilder.build();
	}

	public GetFieldNamesResponse internalGetFieldNames(GetFieldNamesRequest request) throws Exception {
		String indexName = request.getIndexName();
		io.zulia.server.index.ZuliaIndex i = getIndexFromName(indexName);
		return i.getFieldNames();
	}

	public GetTermsResponse getTerms(GetTermsRequest request) throws Exception {

		String indexName = request.getIndexName();
		ZuliaBase.MasterSlaveSettings masterSlaveSettings = request.getMasterSlaveSettings();

		io.zulia.server.index.ZuliaIndex i = getIndexFromName(indexName);

		RequestNodeFederator<GetTermsRequest, InternalGetTermsResponse> federator = new RequestNodeFederator<GetTermsRequest, InternalGetTermsResponse>(
				thisNode, currentOtherNodesActive, masterSlaveSettings, i, pool) {

			@Override
			public InternalGetTermsResponse processExternal(Node node, GetTermsRequest request) throws Exception {
				return internalClient.getTerms(node, request);
			}

			@Override
			public InternalGetTermsResponse processInternal(GetTermsRequest request) throws Exception {
				return internalGetTerms(request);
			}

		};

		List<InternalGetTermsResponse> responses = federator.send(request);

		TreeMap<String, Term.Builder> terms = new TreeMap<>();
		for (InternalGetTermsResponse response : responses) {
			for (GetTermsResponse gtr : response.getGetTermsResponseList()) {
				for (Term term : gtr.getTermList()) {
					String key = term.getValue();
					if (!terms.containsKey(key)) {
						Term.Builder termBuilder = Term.newBuilder().setValue(key).setDocFreq(0).setTermFreq(0);
						termBuilder.setScore(0);
						terms.put(key, termBuilder);
					}
					Term.Builder builder = terms.get(key);
					builder.setDocFreq(builder.getDocFreq() + term.getDocFreq());
					builder.setTermFreq(builder.getTermFreq() + term.getTermFreq());

					builder.setScore(builder.getScore() + term.getScore());

				}
			}
		}

		GetTermsResponse.Builder responseBuilder = GetTermsResponse.newBuilder();

		Term.Builder value = null;

		int count = 0;

		int amount = request.getAmount();
		for (Term.Builder builder : terms.values()) {
			value = builder;
			if (builder.getDocFreq() >= request.getMinDocFreq() && builder.getTermFreq() >= request.getMinTermFreq()) {
				responseBuilder.addTerm(builder.build());
				count++;
			}

			if (amount != 0 && count >= amount) {
				break;
			}
		}

		if (value != null) {
			responseBuilder.setLastTerm(value.build());
		}

		return responseBuilder.build();
	}

	public InternalGetTermsResponse internalGetTerms(GetTermsRequest request) throws Exception {
		String indexName = request.getIndexName();
		io.zulia.server.index.ZuliaIndex i = getIndexFromName(indexName);
		return i.getTerms(request);
	}

	public GetIndexSettingsResponse getIndexSettings(GetIndexSettingsRequest request) throws Exception {
		String indexName = request.getIndexName();
		ZuliaIndex.IndexSettings indexSettings = indexService.getIndex(indexName);
		return GetIndexSettingsResponse.newBuilder().setIndexSettings(indexSettings).build();
	}

	private io.zulia.server.index.ZuliaIndex getIndexFromName(String indexName) throws IndexDoesNotExist {
		io.zulia.server.index.ZuliaIndex i = indexMap.get(indexName);
		if (i == null) {
			throw new IndexDoesNotExist(indexName);
		}
		return i;
	}
}
