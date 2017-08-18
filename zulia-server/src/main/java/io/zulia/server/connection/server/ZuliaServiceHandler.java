package io.zulia.server.connection.server;

import io.grpc.stub.StreamObserver;
import io.zulia.message.ZuliaServiceGrpc;
import io.zulia.message.ZuliaServiceOuterClass.*;
import io.zulia.server.connection.server.handler.*;
import io.zulia.server.index.ZuliaIndexManager;

public class ZuliaServiceHandler extends ZuliaServiceGrpc.ZuliaServiceImplBase {

	private final InternalQueryServerRequest internalQueryServerRequest;
	private final QueryServerRequest queryServerRequest;
	private final StoreServerRequest storeServerRequest;
	private final InternalStoreServerRequest internalStoreServerRequest;
	private final DeleteServerRequest deleteServerRequest;
	private final InternalDeleteServerRequest internalDeleteServerRequest;
	private final BatchDeleteServerRequest batchDeleteServerRequest;
	private final FetchServerRequest fetchServerServerRequest;
	private final InternalFetchServerRequest internalFetchServerServerRequest;
	private final BatchFetchServerRequest batchFetchServerRequest;
	private final CreateIndexServerRequest createIndexServerRequest;
	private final DeleteIndexServerRequest deleteIndexServerRequest;
	private final GetIndexesServerRequest getIndexesServerRequest;
	private final GetNumberOfDocsServerRequest getNumberOfDocsServerRequest;
	private final InternalGetNumberOfDocsServerRequest internalGetNumberOfDocsServerRequest;
	private final ClearServerRequest clearServerRequest;
	private final InternalClearServerRequest internalClearServerRequest;
	private final OptimizeServerRequest optimizeServerRequest;
	private final InternalOptimizeServerRequest internalOptimizeServerRequest;
	private final GetFieldNamesServerRequest getFieldNamesServerRequest;
	private final InternalGetFieldNamesServerRequest internalGetFieldNamesServerRequest;
	private final GetTermsServerRequest getTermsServerRequest;
	private final InternalGetTermsServerRequest internalGetTermsServerRequest;
	private final GetNodesServerRequest getNodesServerRequest;
	private final GetIndexSettingsServerRequest getIndexSettingsServerRequest;

	public ZuliaServiceHandler(ZuliaIndexManager indexManager) {
		internalQueryServerRequest = new InternalQueryServerRequest(indexManager);
		queryServerRequest = new QueryServerRequest(indexManager);
		storeServerRequest = new StoreServerRequest(indexManager);
		internalStoreServerRequest = new InternalStoreServerRequest(indexManager);
		deleteServerRequest = new DeleteServerRequest(indexManager);
		internalDeleteServerRequest = new InternalDeleteServerRequest(indexManager);
		batchDeleteServerRequest = new BatchDeleteServerRequest(indexManager);
		fetchServerServerRequest = new FetchServerRequest(indexManager);
		internalFetchServerServerRequest = new InternalFetchServerRequest(indexManager);
		batchFetchServerRequest = new BatchFetchServerRequest(indexManager);
		createIndexServerRequest = new CreateIndexServerRequest(indexManager);
		deleteIndexServerRequest = new DeleteIndexServerRequest(indexManager);
		getIndexesServerRequest = new GetIndexesServerRequest(indexManager);
		getNumberOfDocsServerRequest = new GetNumberOfDocsServerRequest(indexManager);
		internalGetNumberOfDocsServerRequest = new InternalGetNumberOfDocsServerRequest(indexManager);
		clearServerRequest = new ClearServerRequest(indexManager);
		internalClearServerRequest = new InternalClearServerRequest(indexManager);
		optimizeServerRequest = new OptimizeServerRequest(indexManager);
		internalOptimizeServerRequest = new InternalOptimizeServerRequest(indexManager);
		getFieldNamesServerRequest = new GetFieldNamesServerRequest(indexManager);
		internalGetFieldNamesServerRequest = new InternalGetFieldNamesServerRequest(indexManager);
		getTermsServerRequest = new GetTermsServerRequest(indexManager);
		internalGetTermsServerRequest = new InternalGetTermsServerRequest(indexManager);
		getNodesServerRequest = new GetNodesServerRequest(indexManager);
		getIndexSettingsServerRequest = new GetIndexSettingsServerRequest(indexManager);

	}

	@Override
	public void internalQuery(QueryRequest request, StreamObserver<InternalQueryResponse> responseObserver) {
		internalQueryServerRequest.handleRequest(request, responseObserver);
	}

	@Override
	public void query(QueryRequest request, StreamObserver<QueryResponse> responseObserver) {
		queryServerRequest.handleRequest(request, responseObserver);
	}

	@Override
	public void store(StoreRequest request, StreamObserver<StoreResponse> responseObserver) {
		storeServerRequest.handleRequest(request, responseObserver);
	}

	@Override
	public void internalStore(StoreRequest request, StreamObserver<StoreResponse> responseObserver) {
		internalStoreServerRequest.handleRequest(request, responseObserver);
	}

	@Override
	public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
		deleteServerRequest.handleRequest(request, responseObserver);
	}

	@Override
	public void internalDelete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
		internalDeleteServerRequest.handleRequest(request, responseObserver);
	}

	@Override
	public void batchDelete(BatchDeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
		batchDeleteServerRequest.handleRequest(request, responseObserver);
	}

	@Override
	public void fetch(FetchRequest request, StreamObserver<FetchResponse> responseObserver) {
		fetchServerServerRequest.handleRequest(request, responseObserver);
	}

	@Override
	public void internalFetch(FetchRequest request, StreamObserver<FetchResponse> responseObserver) {
		internalFetchServerServerRequest.handleRequest(request, responseObserver);
	}

	@Override
	public void batchFetch(BatchFetchRequest request, StreamObserver<FetchResponse> responseObserver) {
		batchFetchServerRequest.handleRequest(request, responseObserver);
	}

	@Override
	public void createIndex(CreateIndexRequest request, StreamObserver<CreateIndexResponse> responseObserver) {
		createIndexServerRequest.handleRequest(request, responseObserver);
	}

	@Override
	public void deleteIndex(DeleteIndexRequest request, StreamObserver<DeleteIndexResponse> responseObserver) {
		deleteIndexServerRequest.handleRequest(request, responseObserver);
	}

	@Override
	public void getIndexes(GetIndexesRequest request, StreamObserver<GetIndexesResponse> responseObserver) {
		getIndexesServerRequest.handleRequest(request, responseObserver);
	}

	@Override
	public void getNumberOfDocs(GetNumberOfDocsRequest request, StreamObserver<GetNumberOfDocsResponse> responseObserver) {
		getNumberOfDocsServerRequest.handleRequest(request, responseObserver);
	}

	@Override
	public void internalGetNumberOfDocs(GetNumberOfDocsRequest request, StreamObserver<GetNumberOfDocsResponse> responseObserver) {
		internalGetNumberOfDocsServerRequest.handleRequest(request, responseObserver);
	}

	@Override
	public void clear(ClearRequest request, StreamObserver<ClearResponse> responseObserver) {
		clearServerRequest.handleRequest(request, responseObserver);
	}

	@Override
	public void internalClear(ClearRequest request, StreamObserver<ClearResponse> responseObserver) {
		internalClearServerRequest.handleRequest(request, responseObserver);
	}

	@Override
	public void optimize(OptimizeRequest request, StreamObserver<OptimizeResponse> responseObserver) {
		optimizeServerRequest.handleRequest(request, responseObserver);
	}

	@Override
	public void internalOptimize(OptimizeRequest request, StreamObserver<OptimizeResponse> responseObserver) {
		internalOptimizeServerRequest.handleRequest(request, responseObserver);
	}

	@Override
	public void getFieldNames(GetFieldNamesRequest request, StreamObserver<GetFieldNamesResponse> responseObserver) {
		getFieldNamesServerRequest.handleRequest(request, responseObserver);
	}

	@Override
	public void internalGetFieldNames(GetFieldNamesRequest request, StreamObserver<GetFieldNamesResponse> responseObserver) {
		internalGetFieldNamesServerRequest.handleRequest(request, responseObserver);
	}

	@Override
	public void getTerms(GetTermsRequest request, StreamObserver<GetTermsResponse> responseObserver) {
		getTermsServerRequest.handleRequest(request, responseObserver);
	}

	@Override
	public void internalGetTerms(GetTermsRequest request, StreamObserver<InternalGetTermsResponse> responseObserver) {
		internalGetTermsServerRequest.handleRequest(request, responseObserver);
	}

	@Override
	public void getNodes(GetNodesRequest request, StreamObserver<GetNodesResponse> responseObserver) {
		getNodesServerRequest.handleRequest(request, responseObserver);
	}

	@Override
	public void getIndexSettings(GetIndexSettingsRequest request, StreamObserver<GetIndexSettingsResponse> responseObserver) {
		getIndexSettingsServerRequest.handleRequest(request, responseObserver);
	}
}
