package io.zulia.server.connection.server;

import io.grpc.stub.StreamObserver;
import io.zulia.message.ZuliaServiceGrpc;
import io.zulia.message.ZuliaServiceOuterClass.*;
import io.zulia.server.connection.server.handler.InternalQueryServerRequest;
import io.zulia.server.index.ZuliaIndexManager;

public class ZuliaServiceHandler extends ZuliaServiceGrpc.ZuliaServiceImplBase {

	private final ZuliaIndexManager indexManager;
	private final InternalQueryServerRequest internalQueryServerRequest;

	public ZuliaServiceHandler(ZuliaIndexManager indexManager) {
		this.indexManager = indexManager;
		internalQueryServerRequest = new InternalQueryServerRequest(indexManager);

	}

	@Override
	public void queryInternal(QueryRequest request, StreamObserver<InternalQueryResponse> responseObserver) {
		internalQueryServerRequest.handleRequest(request, responseObserver);
	}

	@Override
	public void query(QueryRequest request, StreamObserver<QueryResponse> responseObserver) {
		//TODO:.....
		super.query(request, responseObserver);
	}

	@Override
	public void store(StoreRequest request, StreamObserver<StoreResponse> responseObserver) {
		super.store(request, responseObserver);
	}

	@Override
	public void delete(DeleteRequest request, StreamObserver<DeleteResponse> responseObserver) {
		super.delete(request, responseObserver);
	}

	@Override
	public void batchDelete(BatchDeleteRequest request, StreamObserver<BatchDeleteResponse> responseObserver) {
		super.batchDelete(request, responseObserver);
	}

	@Override
	public void fetch(FetchRequest request, StreamObserver<FetchResponse> responseObserver) {
		super.fetch(request, responseObserver);
	}

	@Override
	public void batchFetch(BatchFetchRequest request, StreamObserver<BatchFetchResponse> responseObserver) {
		super.batchFetch(request, responseObserver);
	}

	@Override
	public void createIndex(CreateIndexRequest request, StreamObserver<CreateIndexResponse> responseObserver) {
		super.createIndex(request, responseObserver);
	}

	@Override
	public void deleteIndex(DeleteIndexRequest request, StreamObserver<DeleteIndexResponse> responseObserver) {
		super.deleteIndex(request, responseObserver);
	}

	@Override
	public void getIndexes(GetIndexesRequest request, StreamObserver<GetIndexesResponse> responseObserver) {
		super.getIndexes(request, responseObserver);
	}

	@Override
	public void getNumberOfDocs(GetNumberOfDocsRequest request, StreamObserver<GetNumberOfDocsResponse> responseObserver) {
		super.getNumberOfDocs(request, responseObserver);
	}

	@Override
	public void getNumberOfDocsInternal(GetNumberOfDocsRequest request, StreamObserver<GetNumberOfDocsResponse> responseObserver) {
		super.getNumberOfDocsInternal(request, responseObserver);
	}

	@Override
	public void clear(ClearRequest request, StreamObserver<ClearResponse> responseObserver) {
		super.clear(request, responseObserver);
	}

	@Override
	public void clearInternal(ClearRequest request, StreamObserver<ClearResponse> responseObserver) {
		super.clearInternal(request, responseObserver);
	}

	@Override
	public void optimize(OptimizeRequest request, StreamObserver<OptimizeResponse> responseObserver) {
		super.optimize(request, responseObserver);
	}

	@Override
	public void optimizeInternal(OptimizeRequest request, StreamObserver<OptimizeResponse> responseObserver) {
		super.optimizeInternal(request, responseObserver);
	}

	@Override
	public void getFieldNames(GetFieldNamesRequest request, StreamObserver<GetFieldNamesResponse> responseObserver) {
		super.getFieldNames(request, responseObserver);
	}

	@Override
	public void getFieldNamesInternal(GetFieldNamesRequest request, StreamObserver<GetFieldNamesResponse> responseObserver) {
		super.getFieldNamesInternal(request, responseObserver);
	}

	@Override
	public void getTerms(GetTermsRequest request, StreamObserver<GetTermsResponse> responseObserver) {
		super.getTerms(request, responseObserver);
	}

	@Override
	public void getTermsInternal(GetTermsRequest request, StreamObserver<GetTermsResponseInternal> responseObserver) {
		super.getTermsInternal(request, responseObserver);
	}

	@Override
	public void getNodes(GetNodesRequest request, StreamObserver<GetNodesResponse> responseObserver) {
		super.getNodes(request, responseObserver);
	}

	@Override
	public void getIndexSettings(GetIndexSettingsRequest request, StreamObserver<GetIndexSettingsResponse> responseObserver) {
		super.getIndexSettings(request, responseObserver);
	}
}
