package io.zulia.client.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.zulia.cache.DocId;
import io.zulia.client.command.BatchFetch;
import io.zulia.client.command.FetchDocument;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.client.result.BatchFetchResult;
import io.zulia.client.result.FetchResult;
import io.zulia.client.result.QueryResult;
import io.zulia.message.ZuliaQuery.ScoredResult;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Caches zulia result documents.  Timestamp associated with the result document can be used to match searches with result documents
 * @author mdavis
 *
 */
public class ClientDocumentCache {
	private ZuliaWorkPool zuliaWorkPool;

	private static Cache<DocId, FetchResult> documentCache;

	public ClientDocumentCache(ZuliaWorkPool zuliaWorkPool, int maxSize) {
		this.zuliaWorkPool = zuliaWorkPool;
		documentCache = CacheBuilder.newBuilder().concurrencyLevel(16).maximumSize(maxSize).build();
	}

	/**
	 * Returns the last cached version of the result document or fetches if the result document is not in the cache
	 * @param uniqueId - uniqueId to fetch
	 * @return FetchResult
	 * @throws Exception
	 */
	public FetchResult fetch(String uniqueId, String indexName) throws Exception {
		return fetch(uniqueId, indexName, null);
	}

	/**
	 * Returns the last cached version of the result document if timestamp matches, fetches if the document is not in the cache or if the timestamp do not match
	 * @param scoredResult - scored result returned from a search
	 * @return FetchResult
	 * @throws Exception
	 */
	public FetchResult fetch(ScoredResult scoredResult) throws Exception {
		return fetch(scoredResult.getUniqueId(), scoredResult.getIndexName(), scoredResult.getTimestamp());
	}

	/**
	 * Returns the last cached version of the result document if timestamp matches, fetches if the document is not in the cache or if the timestamp do not match
	 * @param uniqueId - uniqueId to fetch
	 * @param timestamp - timestamp to check against
	 * @return FetchResult
	 * @throws Exception
	 */
	public FetchResult fetch(String uniqueId, String indexName, Long timestamp) throws Exception {
		FetchResult fr = documentCache.getIfPresent(new DocId(uniqueId, indexName));

		boolean fetch = fetchNeeded(fr, timestamp);

		if (fetch) {
			FetchDocument fetchDocument = new FetchDocument(uniqueId, indexName);
			fetchDocument.setTimestamp(timestamp);
			fr = zuliaWorkPool.fetch(fetchDocument);
			if (fr.hasResultDocument()) {
				documentCache.put(new DocId(uniqueId, indexName), fr);

			}
		}

		return fr;
	}

	/**
	 * Returns the last cached version of the result documents if timestamp matches in batch.
	 * @param queryResult - query result with the documents to fetch.
	 * @return BatchFetchResult
	 * @throws Exception
	 */
	public BatchFetchResult fetch(QueryResult queryResult) throws Exception {
		return fetch(queryResult.getResults());
	}

	private BatchFetchResult fetch(Collection<ScoredResult> scoredResults) throws Exception {

		List<FetchResult> resultsFromCache = new ArrayList<>();
		List<FetchDocument> fetchDocumentList = new ArrayList<>();

		for (ScoredResult sr : scoredResults) {
			FetchResult fetchResult = documentCache.getIfPresent(new DocId(sr.getUniqueId(), sr.getIndexName()));
			boolean fetch = fetchNeeded(fetchResult, sr.getTimestamp());

			if (fetch) {
				FetchDocument fetchDocument = new FetchDocument(sr.getUniqueId(), sr.getIndexName());
				fetchDocument.setTimestamp(sr.getTimestamp());
				fetchDocumentList.add(fetchDocument);
			}
			else {
				resultsFromCache.add(fetchResult);
			}
		}

		if (!fetchDocumentList.isEmpty()) {
			BatchFetchResult bfr = zuliaWorkPool.batchFetch(new BatchFetch().addFetches(fetchDocumentList));
			bfr.getFetchResults().stream().filter(FetchResult::hasResultDocument)
					.forEach(fr -> documentCache.put(new DocId(fr.getUniqueId(), fr.getIndexName()), fr));

			bfr.getFetchResults().addAll(resultsFromCache);
			return bfr;
		}
		else {
			return new BatchFetchResult(resultsFromCache);
		}

	}

	/**
	 *
	 * @param fetchResult
	 * @param timestamp
	 * @return whether or not we need to fetch more
	 */
	private boolean fetchNeeded(FetchResult fetchResult, Long timestamp) {
		boolean fetch = false;

		if (fetchResult == null) { //no result in cache - fetch regardless of passed time stamp
			fetch = true;
		}
		else if (timestamp != null) { //asking for a specific version and found a version in cache
			if (fetchResult.getDocumentTimestamp() == null) { //no document in cache and asking for document with a timestamp
				fetch = true;
			}
			else if (Long.compare(fetchResult.getDocumentTimestamp(), timestamp) != 0) { //outdated document in cache
				fetch = true;
			}
		}
		return fetch;
	}
}
