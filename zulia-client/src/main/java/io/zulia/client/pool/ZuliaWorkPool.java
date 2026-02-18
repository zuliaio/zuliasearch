package io.zulia.client.pool;

import com.google.common.util.concurrent.ListenableFuture;
import io.zulia.client.command.*;
import io.zulia.client.command.builder.Search;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.config.ZuliaPoolConfig;
import io.zulia.client.result.*;
import io.zulia.fields.GsonDocumentMapper;
import io.zulia.message.ZuliaIndex.IndexAlias;
import io.zulia.message.ZuliaQuery.ScoredResult;
import io.zulia.util.ResultHelper;
import org.bson.Document;

import java.util.List;
import java.util.function.Consumer;

public class ZuliaWorkPool extends ZuliaBaseWorkPool {

	public ZuliaWorkPool(ZuliaPoolConfig zuliaPoolConfig) {
		super(zuliaPoolConfig);
	}

	public BatchFetchResult batchFetch(BatchFetch batchFetch) throws Exception {
		return execute(batchFetch);
	}

	public ListenableFuture<BatchFetchResult> batchFetchAsync(BatchFetch batchFetch) {
		return executeAsync(batchFetch);
	}

	public ClearIndexResult clearIndex(String index) throws Exception {
		return clearIndex(new ClearIndex(index));
	}

	public ClearIndexResult clearIndex(ClearIndex clearIndex) throws Exception {
		return execute(clearIndex);
	}

	public ListenableFuture<ClearIndexResult> clearIndexAsync(String index) throws Exception {
		return clearIndexAsync(new ClearIndex(index));
	}

	public ListenableFuture<ClearIndexResult> clearIndexAsync(ClearIndex clearIndex) {
		return executeAsync(clearIndex);
	}

	public CreateIndexResult createIndex(CreateIndex createIndex) throws Exception {
		return execute(createIndex);
	}

	public CreateIndexResult createIndex(ClientIndexConfig indexConfig) throws Exception {
		return execute(new CreateIndex(indexConfig));
	}

	public CreateIndexAliasResult createIndexAlias(CreateIndexAlias createIndexAlias) throws Exception {
		return execute(createIndexAlias);
	}

	public CreateIndexAliasResult createIndexAlias(String aliasName, String indexName) throws Exception {
		return execute(new CreateIndexAlias(aliasName, indexName));
	}

	public ListenableFuture<CreateIndexResult> createIndexAsync(CreateIndex createIndex) {
		return executeAsync(createIndex);
	}

	public ListenableFuture<CreateIndexResult> createIndexAsync(ClientIndexConfig indexConfig) {
		return executeAsync(new CreateIndex(indexConfig));
	}

	public UpdateIndexResult updateIndex(UpdateIndex updateIndex) throws Exception {
		return execute(updateIndex);
	}

	public ListenableFuture<UpdateIndexResult> updateIndexAsync(UpdateIndex updateIndex) {
		return executeAsync(updateIndex);
	}

	public DeleteResult delete(Delete delete) throws Exception {
		return execute(delete);
	}

	public ListenableFuture<DeleteResult> deleteAsync(Delete delete) {
		return executeAsync(delete);
	}

	public BatchDeleteResult batchDelete(BatchDelete batchDelete) throws Exception {
		return execute(batchDelete);
	}

	public ListenableFuture<BatchDeleteResult> batchDeleteAsync(BatchDelete batchDelete) {
		return executeAsync(batchDelete);
	}

	public DeleteIndexResult deleteIndex(String indexName) throws Exception {
		return execute(new DeleteIndex(indexName));
	}

	public DeleteIndexResult deleteIndexAndAssociatedFiles(String indexName) throws Exception {
		return execute(new DeleteIndex(indexName).setDeleteAssociated(true));
	}

	public DeleteIndexAliasResult deleteIndexAlias(String aliasName) throws Exception {
		return execute(new DeleteIndexAlias(aliasName));
	}

	public ListenableFuture<DeleteIndexResult> deleteIndexAsync(String indexName) {
		return executeAsync(new DeleteIndex(indexName));
	}

	public DeleteIndexAliasResult deleteIndexAlias(DeleteIndexAlias deleteIndexAlias) throws Exception {
		return execute(deleteIndexAlias);
	}

	public DeleteIndexResult deleteIndex(DeleteIndex deleteIndex) throws Exception {
		return execute(deleteIndex);
	}

	public ListenableFuture<DeleteIndexResult> deleteIndexAsync(DeleteIndex deleteIndex) {
		return executeAsync(deleteIndex);
	}

	public FetchResult fetch(Fetch fetch) throws Exception {
		return execute(fetch);
	}

	public ListenableFuture<FetchResult> fetchAsync(Fetch fetch) {
		return executeAsync(fetch);
	}

	public FetchLargeAssociatedResult fetchLargeAssociated(FetchLargeAssociated fetchLargeAssociated) throws Exception {
		return execute(fetchLargeAssociated);
	}

	public ListenableFuture<FetchLargeAssociatedResult> fetchLargeAssociatedAsync(FetchLargeAssociated fetchLargeAssociated) {
		return executeAsync(fetchLargeAssociated);
	}

	public GetFieldsResult getFields(String indexName) throws Exception {
		return getFields(new GetFields(indexName));
	}

	public GetFieldsResult getFields(GetFields getFields) throws Exception {
		return execute(getFields);
	}

	public ListenableFuture<GetFieldsResult> getFieldsAsync(GetFields getFields) {
		return executeAsync(getFields);
	}

	public GetIndexesResult getIndexes() throws Exception {
		return execute(new GetIndexes());
	}

	public ListenableFuture<GetIndexesResult> getIndexesAsync() {
		return executeAsync(new GetIndexes());
	}

	public GetNodesResult getNodes() throws Exception {
		return execute(new GetNodes());
	}

	public GetNodesResult getActiveNodes() throws Exception {
		return execute(new GetNodes().setActiveOnly(true));
	}

	public ListenableFuture<GetNodesResult> getNodesAsync() {
		return executeAsync(new GetNodes());
	}

	public GetStatsResult getStats() throws Exception {
		return execute(new GetStats());
	}

	public ListenableFuture<GetStatsResult> getStatsAsync() {
		return executeAsync(new GetStats());
	}

	public GetNumberOfDocsResult getNumberOfDocs(String indexName) throws Exception {
		return getNumberOfDocs(new GetNumberOfDocs(indexName));
	}

	public GetNumberOfDocsResult getNumberOfDocs(GetNumberOfDocs getNumberOfDocs) throws Exception {
		return execute(getNumberOfDocs);
	}

	public ListenableFuture<GetNumberOfDocsResult> getNumberOfDocsAsync(String indexName) {
		return executeAsync(new GetNumberOfDocs(indexName));
	}

	public ListenableFuture<GetNumberOfDocsResult> getNumberOfDocsAsync(GetNumberOfDocs getNumberOfDocs) {
		return executeAsync(getNumberOfDocs);
	}

	public GetTermsResult getTerms(GetTerms getTerms) throws Exception {
		return execute(getTerms);
	}

	public ListenableFuture<GetTermsResult> getTermsAsync(GetTerms getTerms) {
		return executeAsync(getTerms);
	}

	public OptimizeIndexResult optimizeIndex(OptimizeIndex optimizeIndex) throws Exception {
		return execute(optimizeIndex);
	}

	public OptimizeIndexResult optimizeIndex(String index) throws Exception {
		return optimizeIndex(new OptimizeIndex(index));
	}

	public ListenableFuture<OptimizeIndexResult> optimizeIndexAsync(String index) throws Exception {
		return optimizeIndexAsync(new OptimizeIndex(index));
	}

	public ListenableFuture<OptimizeIndexResult> optimizeIndexAsync(OptimizeIndex optimizeIndex) {
		return executeAsync(optimizeIndex);
	}

	public SearchResult search(Search search) throws Exception {
		return execute(search);
	}

	public <T> void searchAllAsMappedDocument(Search search, GsonDocumentMapper<T> mapper, Consumer<T> mappedDocumentHandler) throws Exception {
		searchAllAsScoredResult(search, scoredResult -> {
			try {
				mappedDocumentHandler.accept(mapper.fromScoredResult(scoredResult));
			}
			catch (Exception e) {
				throw new RuntimeException(e);
			}
		});
	}

	public void searchAllAsDocument(Search search, Consumer<Document> documentHandler) throws Exception {
		searchAllAsScoredResult(search, scoredResult -> documentHandler.accept(ResultHelper.getDocumentFromScoredResult(scoredResult)));
	}

	public void searchAllAsScoredResult(Search search, Consumer<ScoredResult> scoredResultHandler) throws Exception {
		searchAll(search, queryResult -> queryResult.getResults().forEach(scoredResultHandler));
	}

	public void searchAllAsCompleteResult(Search search, Consumer<CompleteResult> completeResultHandler) throws Exception {
		searchAll(search, queryResult -> queryResult.getCompleteResults().forEach(completeResultHandler));
	}

	public void searchAll(Search search, Consumer<SearchResult> resultHandler) throws Exception {

		if (search.getAmount() <= 0) {
			throw new IllegalArgumentException("Amount must be set to page size for search all");
		}

		SearchResult searchResult = search(search);
		while (searchResult.hasResults()) {
			resultHandler.accept(searchResult);
			search.setLastResult(searchResult);
			searchResult = search(search);
		}
	}

	public ListenableFuture<SearchResult> searchAsync(Search search) {
		return executeAsync(search);
	}

	public StoreResult store(Store store) throws Exception {
		return execute(store);
	}

	public ListenableFuture<StoreResult> storeAsync(Store store) {
		return executeAsync(store);
	}

	public StoreLargeAssociatedResult storeLargeAssociated(StoreLargeAssociated storeLargeAssociated) throws Exception {
		return execute(storeLargeAssociated);
	}

	public ListenableFuture<StoreLargeAssociatedResult> storeLargeAssociatedAsync(StoreLargeAssociated storeLargeAssociated) {
		return executeAsync(storeLargeAssociated);
	}

	public void updateNodes() throws Exception {
		GetNodesResult getNodesResult = execute(new GetNodes().setActiveOnly(true));
		updateNodes(getNodesResult.getNodes());
	}

	public GetIndexConfigResult getIndexConfig(String indexName) throws Exception {
		return getIndexConfig(new GetIndexConfig(indexName));
	}

	public GetIndexConfigResult getIndexConfig(GetIndexConfig getIndexConfig) throws Exception {
		return execute(getIndexConfig);
	}

	public ListenableFuture<GetIndexConfigResult> getIndexConfigAsync(GetIndexConfig getIndexConfig) {
		return executeAsync(getIndexConfig);
	}

	public ReindexResult reindex(String index) throws Exception {
		return execute(new Reindex(index));
	}

	public ReindexResult reindex(Reindex reindex) throws Exception {
		return execute(reindex);
	}

	public ListenableFuture<ReindexResult> reindexAsync(Reindex reindex) {
		return executeAsync(reindex);
	}

	public ListenableFuture<ReindexResult> reindexAsync(String index) throws Exception {
		return reindexAsync(new Reindex(index));
	}

	public List<IndexAlias> getIndexAliases() throws Exception {
		return getNodes().getIndexAliases();
	}

}
