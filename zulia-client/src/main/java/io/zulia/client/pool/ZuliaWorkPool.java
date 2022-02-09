package io.zulia.client.pool;

import com.google.common.util.concurrent.ListenableFuture;
import io.zulia.client.command.*;
import io.zulia.client.command.builder.Search;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.config.ZuliaPoolConfig;
import io.zulia.client.result.*;
import io.zulia.fields.Mapper;
import io.zulia.message.ZuliaQuery.ScoredResult;
import io.zulia.util.ResultHelper;
import org.bson.Document;

import java.util.function.Consumer;

public class ZuliaWorkPool extends ZuliaBaseWorkPool {

	public ZuliaWorkPool(ZuliaPoolConfig zuliaPoolConfig) throws Exception {
		super(zuliaPoolConfig);
	}

	public BatchFetchResult batchFetch(BatchFetch batchFetch) throws Exception {
		return execute(batchFetch);
	}

	public ListenableFuture<BatchFetchResult> batchFetchAsync(BatchFetch batchFetch) throws Exception {
		return executeAsync(batchFetch);
	}

	public ClearIndexResult clearIndex(ClearIndex clearIndex) throws Exception {
		return execute(clearIndex);
	}

	public ListenableFuture<ClearIndexResult> clearIndexAsync(ClearIndex clearIndex) throws Exception {
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

	public ListenableFuture<CreateIndexResult> createIndexAsync(CreateIndex createIndex) throws Exception {
		return executeAsync(createIndex);
	}

	public ListenableFuture<CreateIndexResult> createIndexAsync(ClientIndexConfig indexConfig) throws Exception {
		return executeAsync(new CreateIndex(indexConfig));
	}

	public DeleteResult delete(Delete delete) throws Exception {
		return execute(delete);
	}

	public ListenableFuture<DeleteResult> deleteAsync(Delete delete) throws Exception {
		return executeAsync(delete);
	}

	public BatchDeleteResult batchDelete(BatchDelete batchDelete) throws Exception {
		return execute(batchDelete);
	}

	public ListenableFuture<BatchDeleteResult> batchDeleteAsync(BatchDelete batchDelete) throws Exception {
		return executeAsync(batchDelete);
	}

	public DeleteIndexResult deleteIndex(String indexName) throws Exception {
		return execute(new DeleteIndex(indexName));
	}

	public DeleteIndexAliasResult deleteIndexAlias(String aliasName) throws Exception {
		return execute(new DeleteIndexAlias(aliasName));
	}

	public ListenableFuture<DeleteIndexResult> deleteIndexAsync(String indexName) throws Exception {
		return executeAsync(new DeleteIndex(indexName));
	}

	public DeleteIndexAliasResult deleteIndexAlias(DeleteIndexAlias deleteIndexAlias) throws Exception {
		return execute(deleteIndexAlias);
	}

	public DeleteIndexResult deleteIndex(DeleteIndex deleteIndex) throws Exception {
		return execute(deleteIndex);
	}

	public ListenableFuture<DeleteIndexResult> deleteIndexAsync(DeleteIndex deleteIndex) throws Exception {
		return executeAsync(deleteIndex);
	}

	public FetchResult fetch(Fetch fetch) throws Exception {
		return execute(fetch);
	}

	public ListenableFuture<FetchResult> fetchAsync(Fetch fetch) throws Exception {
		return executeAsync(fetch);
	}

	public FetchLargeAssociatedResult fetchLargeAssociated(FetchLargeAssociated fetchLargeAssociated) throws Exception {
		return execute(fetchLargeAssociated);
	}

	public ListenableFuture<FetchLargeAssociatedResult> fetchLargeAssociatedAsync(FetchLargeAssociated fetchLargeAssociated) throws Exception {
		return executeAsync(fetchLargeAssociated);
	}

	public GetFieldsResult getFields(String indexName) throws Exception {
		return getFields(new GetFields(indexName));
	}

	public GetFieldsResult getFields(GetFields getFields) throws Exception {
		return execute(getFields);
	}

	public ListenableFuture<GetFieldsResult> getFieldsAsync(GetFields getFields) throws Exception {
		return executeAsync(getFields);
	}

	public GetIndexesResult getIndexes() throws Exception {
		return execute(new GetIndexes());
	}

	public ListenableFuture<GetIndexesResult> getIndexesAsync() throws Exception {
		return executeAsync(new GetIndexes());
	}

	public GetNodesResult getNodes() throws Exception {
		return execute(new GetNodes());
	}

	public ListenableFuture<GetNodesResult> getNodesAsync() throws Exception {
		return executeAsync(new GetNodes());
	}

	public GetNumberOfDocsResult getNumberOfDocs(String indexName) throws Exception {
		return getNumberOfDocs(new GetNumberOfDocs(indexName));
	}

	public GetNumberOfDocsResult getNumberOfDocs(GetNumberOfDocs getNumberOfDocs) throws Exception {
		return execute(getNumberOfDocs);
	}

	public ListenableFuture<GetNumberOfDocsResult> getNumberOfDocsAsync(String indexName) throws Exception {
		return executeAsync(new GetNumberOfDocs(indexName));
	}

	public ListenableFuture<GetNumberOfDocsResult> getNumberOfDocsAsync(GetNumberOfDocs getNumberOfDocs) throws Exception {
		return executeAsync(getNumberOfDocs);
	}

	public GetTermsResult getTerms(GetTerms getTerms) throws Exception {
		return execute(getTerms);
	}

	public ListenableFuture<GetTermsResult> getTermsAsync(GetTerms getTerms) throws Exception {
		return executeAsync(getTerms);
	}

	public OptimizeIndexResult optimizeIndex(OptimizeIndex optimizeIndex) throws Exception {
		return execute(optimizeIndex);
	}

	public ListenableFuture<OptimizeIndexResult> optimizeIndexAsync(OptimizeIndex optimizeIndex) throws Exception {
		return executeAsync(optimizeIndex);
	}

	public QueryResult query(Query query) throws Exception {
		return execute(query);
	}

	public SearchResult search(Search search) throws Exception {
		return execute(search);
	}

	public <T> void queryAllAsMappedDocument(Query query, Mapper<T> mapper, Consumer<T> mappedDocumentHandler) throws Exception {
		queryAllAsScoredResult(query, scoredResult -> {
			try {
				mappedDocumentHandler.accept(mapper.fromScoredResult(scoredResult));
			}
			catch (Exception e) {
				throw new RuntimeException(e);
			}
		});
	}

	public <T> void searchAllAsMappedDocument(Search search, Mapper<T> mapper, Consumer<T> mappedDocumentHandler) throws Exception {
		searchAllAsScoredResult(search, scoredResult -> {
			try {
				mappedDocumentHandler.accept(mapper.fromScoredResult(scoredResult));
			}
			catch (Exception e) {
				throw new RuntimeException(e);
			}
		});
	}

	public void queryAllAsDocument(Query query, Consumer<Document> documentHandler) throws Exception {
		queryAllAsScoredResult(query, scoredResult -> documentHandler.accept(ResultHelper.getDocumentFromScoredResult(scoredResult)));
	}

	public void searchAllAsDocument(Search search, Consumer<Document> documentHandler) throws Exception {
		searchAllAsScoredResult(search, scoredResult -> documentHandler.accept(ResultHelper.getDocumentFromScoredResult(scoredResult)));
	}

	public void queryAllAsScoredResult(Query query, Consumer<ScoredResult> scoredResultHandler) throws Exception {
		queryAll(query, queryResult -> queryResult.getResults().forEach(scoredResultHandler));
	}

	public void searchAllAsScoredResult(Search search, Consumer<ScoredResult> scoredResultHandler) throws Exception {
		searchAll(search, queryResult -> queryResult.getResults().forEach(scoredResultHandler));
	}

	public void queryAll(Query query, Consumer<QueryResult> resultHandler) throws Exception {
		QueryResult queryResult = query(query);
		while (queryResult.hasResults()) {
			resultHandler.accept(queryResult);
			query.setLastResult(queryResult);
			queryResult = query(query);
		}
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

	public ListenableFuture<QueryResult> queryAsync(Query query) throws Exception {
		return executeAsync(query);
	}

	public ListenableFuture<SearchResult> searchAsync(Search search) throws Exception {
		return executeAsync(search);
	}

	public StoreResult store(Store store) throws Exception {
		return execute(store);
	}

	public ListenableFuture<StoreResult> storeAsync(Store store) throws Exception {
		return executeAsync(store);
	}

	public StoreLargeAssociatedResult storeLargeAssociated(StoreLargeAssociated storeLargeAssociated) throws Exception {
		return execute(storeLargeAssociated);
	}

	public ListenableFuture<StoreLargeAssociatedResult> storeLargeAssociatedAsync(StoreLargeAssociated storeLargeAssociated) throws Exception {
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

	public ListenableFuture<GetIndexConfigResult> getIndexConfigAsync(GetIndexConfig getIndexConfig) throws Exception {
		return executeAsync(getIndexConfig);
	}

	public ReindexResult reindex(Reindex reindex) throws Exception {
		return execute(reindex);
	}

	public ListenableFuture<ReindexResult> reindexAsync(Reindex reindex) throws Exception {
		return executeAsync(reindex);
	}

}
