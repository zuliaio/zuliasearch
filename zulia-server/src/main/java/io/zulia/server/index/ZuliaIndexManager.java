package io.zulia.server.index;

import io.zulia.message.ZuliaBase.Node;
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

import static io.zulia.message.ZuliaServiceOuterClass.FetchRequest;
import static io.zulia.message.ZuliaServiceOuterClass.FetchResponse;
import static io.zulia.message.ZuliaServiceOuterClass.GetFieldNamesRequest;
import static io.zulia.message.ZuliaServiceOuterClass.GetFieldNamesResponse;
import static io.zulia.message.ZuliaServiceOuterClass.GetIndexesRequest;
import static io.zulia.message.ZuliaServiceOuterClass.GetIndexesResponse;
import static io.zulia.message.ZuliaServiceOuterClass.GetNodesRequest;
import static io.zulia.message.ZuliaServiceOuterClass.GetNodesResponse;
import static io.zulia.message.ZuliaServiceOuterClass.GetTermsRequest;
import static io.zulia.message.ZuliaServiceOuterClass.GetTermsResponse;
import static io.zulia.message.ZuliaServiceOuterClass.QueryRequest;
import static io.zulia.message.ZuliaServiceOuterClass.QueryResponse;

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

	public QueryResponse query(QueryRequest queryRequest) {
		return null;
	}

	public GetIndexesResponse getIndexes(GetIndexesRequest getIndexesRequest) {
		return null;
	}

	public GetFieldNamesResponse getFieldNames(GetFieldNamesRequest getFieldNamesRequest) {
		return null;
	}

	public FetchResponse fetch(FetchRequest fetchRequest) {
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

	public GetNodesResponse getNodes(GetNodesRequest getNodesRequest) {
		return null;
	}

	public GetTermsResponse getTerms(GetTermsRequest getTermsRequest) {
		return null;
	}
}
