package io.zulia.server.index;

import io.zulia.message.ZuliaBase.Node;
import io.zulia.message.ZuliaServiceOuterClass.*;
import io.zulia.server.config.IndexService;
import io.zulia.server.config.ZuliaConfig;
import io.zulia.server.config.cluster.MongoIndexService;
import io.zulia.server.config.single.FSIndexService;
import io.zulia.server.util.MongoProvider;
import org.bson.Document;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.logging.Logger;

public class ZuliaIndexManager {

	private static final Logger LOG = Logger.getLogger(ZuliaIndexManager.class.getName());

	private final IndexService indexService;

	public ZuliaIndexManager(ZuliaConfig zuliaConfig) {

		if (zuliaConfig.isCluster()) {
			indexService = new MongoIndexService(MongoProvider.getMongoClient());
		}
		else {
			indexService = new FSIndexService(zuliaConfig);
		}
	}

	public void handleNodeAdded(Collection<Node> currentOtherNodesActive, Node nodeAdded) {
		System.out.println("nodeAdded: " + nodeAdded.getServerAddress() + ":" + nodeAdded.getServicePort());
	}

	public void handleNodeRemoved(Collection<Node> currentOtherNodesActive, Node nodeRemoved) {
		System.out.println("nodeRemoved: " + nodeRemoved.getServerAddress() + ":" + nodeRemoved.getServicePort());
	}

	public void shutdown() {

	}

	public void loadIndexes() {

	}

	public void openConnections(Collection<Node> nodes) {

	}

	public GetIndexesResponse getIndexes(GetIndexesRequest request) {
		return null;
	}

	public FetchResponse fetch(FetchRequest request) {
		return null;
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

	public OptimizeResponse internalOptimize(OptimizeRequest request) {
		return null;
	}

	public GetFieldNamesResponse getFieldNames(GetFieldNamesRequest request) {
		return null;
	}

	public GetFieldNamesResponse internalGetFieldNames(GetFieldNamesRequest request) {
		return null;
	}

	public GetTermsResponse getTerms(GetTermsRequest request) {
		return null;
	}

	public InternalGetTermsResponse internalGetTerms(GetTermsRequest request) {
		return null;
	}

	public GetIndexSettingsResponse getIndexSettings(GetIndexSettingsRequest request) {
		return null;
	}
}
