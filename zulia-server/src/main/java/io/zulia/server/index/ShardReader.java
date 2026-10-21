package io.zulia.server.index;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Policy;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import io.zulia.ZuliaFieldConstants;
import io.zulia.message.ZuliaBase;
import io.zulia.message.ZuliaIndex;
import io.zulia.message.ZuliaIndex.AnalyzerSettings;
import io.zulia.message.ZuliaIndex.FieldConfig.FieldType;
import io.zulia.message.ZuliaQuery;
import io.zulia.message.ZuliaServiceOuterClass;
import io.zulia.server.analysis.ZuliaFieldAnalyzer;
import io.zulia.server.analysis.ZuliaPerFieldAnalyzer;
import io.zulia.server.analysis.highlight.ZuliaHighlighter;
import io.zulia.server.analysis.similarity.ConstantSimilarity;
import io.zulia.server.analysis.similarity.TFSimilarity;
import io.zulia.server.config.IndexFieldInfo;
import io.zulia.server.config.ServerIndexConfig;
import io.zulia.server.config.SortFieldInfo;
import io.zulia.server.exceptions.WrappedCheckedException;
import io.zulia.server.field.FieldTypeUtil;
import io.zulia.server.search.GeoDistUtil;
import io.zulia.server.search.QueryCacheKey;
import io.zulia.server.search.ShardQuery;
import io.zulia.server.search.aggregation.AggregationHandler;
import io.zulia.server.search.aggregation.AggregationSettings;
import io.zulia.server.search.distinct.DistinctCountCollectorManager;
import io.zulia.server.search.distinct.DistinctValueCollector;
import io.zulia.util.pool.SemaphoreLimitedVirtualPool;
import io.zulia.util.pool.VirtualThreadPerTaskTaskExecutor;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollectorManager;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.search.highlight.Fragmenter;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.apache.lucene.search.highlight.SimpleSpanFragmenter;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class ShardReader implements AutoCloseable {

	private final static Logger LOG = LoggerFactory.getLogger(ShardReader.class);
	private final DirectoryReader indexReader;
	private final DirectoryTaxonomyReader taxoReader;
	private final ServerIndexConfig indexConfig;
	private final String indexName;
	private final int shardNumber;
	private final long creationTime;
	private final ZuliaPerFieldAnalyzer zuliaPerFieldAnalyzer;
	private final ExecutorService segmentOpenExecutor;
	// the general cache is rebuilt from the index settings on an update, so a request thread reads these once per call
	private volatile AsyncCache<@NotNull QueryCacheKey, ZuliaQuery.ShardQueryResponse> queryResultCache;
	private final AsyncCache<@NotNull QueryCacheKey, ZuliaQuery.ShardQueryResponse> pinnedQueryResultCache;
	private final AggregationSettings aggregationSettings;

	/**
	 * Bytes held by the entry bound general cache. Null when the cache is weighted, since Caffeine then keeps the figure.
	 */
	private volatile AtomicLong queryResultCacheSize;
	private final AtomicLong pinnedQueryResultCacheSize = new AtomicLong();

	private final ConcurrentHashMap<Integer, Integer> dimensionChildCountCache = new ConcurrentHashMap<>();

	private static final String[] EMPTY_STRING_ARRAY = new String[0];

	/**
	 * Used when IndexSettings.shardQueryCacheMaxResponseMB is 0, which is never stamped so an index that keeps the default stays readable by older nodes.
	 */
	public static final int DEFAULT_SHARD_QUERY_CACHE_MAX_RESPONSE_MB = 8;

	private volatile long maxCachedResponseBytes;

	public ShardReader(int shardNumber, DirectoryReader indexReader, DirectoryTaxonomyReader taxoReader, ServerIndexConfig indexConfig,
			ZuliaPerFieldAnalyzer zuliaPerFieldAnalyzer, ExecutorService segmentOpenExecutor, AggregationSettings aggregationSettings) {
		this.creationTime = System.currentTimeMillis();
		this.shardNumber = shardNumber;
		this.indexReader = indexReader;
		this.taxoReader = taxoReader;
		this.indexConfig = indexConfig;
		this.indexName = indexConfig.getIndexName();
		this.zuliaPerFieldAnalyzer = zuliaPerFieldAnalyzer;
		this.segmentOpenExecutor = segmentOpenExecutor;
		this.aggregationSettings = aggregationSettings;
		this.pinnedQueryResultCache = Caffeine.newBuilder().recordStats().buildAsync();
		buildQueryResultCache();
	}

	/**
	 * Builds the general cache from the current index settings. Called at construction and again when the settings change.
	 */
	private void buildQueryResultCache() {
		ZuliaIndex.IndexSettings settings = indexConfig.getIndexSettings();
		Caffeine<Object, Object> cacheBuilder = Caffeine.newBuilder().recordStats();
		long cacheMB = settings.getShardQueryCacheMB();
		if (cacheMB > 0) {
			// a byte budget, since a response with a distinct count or an unlimited facet can be many MB while most are a few KB.
			// Caffeine tracks the weighted size itself, so the counter stays null and the stats read it from the policy
			queryResultCacheSize = null;
			queryResultCache = cacheBuilder.maximumWeight(cacheMB * 1024 * 1024)
					.weigher((QueryCacheKey _, ZuliaQuery.ShardQueryResponse value) -> value.getSerializedSize()).buildAsync();
		}
		else {
			// the listener holds its own counter, so one firing late on a replaced cache cannot touch the counter of the new one
			AtomicLong counter = new AtomicLong();
			queryResultCacheSize = counter;
			RemovalListener<QueryCacheKey, ZuliaQuery.ShardQueryResponse> removalListener = (key, value, cause) -> {
				// Caffeine passes a null value for an entry whose value was collected, which strong references rule out, but the contract allows it
				if (value != null) {
					counter.getAndAdd(-value.getSerializedSize());
				}
			};
			queryResultCache = cacheBuilder.maximumSize(settings.getShardQueryCacheSize()).removalListener(removalListener).buildAsync();
		}
		long maxResponseMB = settings.getShardQueryCacheMaxResponseMB();
		if (maxResponseMB <= 0) {
			maxResponseMB = DEFAULT_SHARD_QUERY_CACHE_MAX_RESPONSE_MB;
		}
		maxCachedResponseBytes = maxResponseMB * 1024 * 1024;
	}

	@Override
	public void close() throws Exception {
		indexReader.close();
		taxoReader.close();
	}

	public long getCreationTime() {
		return creationTime;
	}

	public int getTotalFacets() {
		return taxoReader.getSize();
	}

	public int getDimensionChildCount(int dimOrd) {
		return dimensionChildCountCache.computeIfAbsent(dimOrd, ord -> {
			try {
				int count = 0;
				TaxonomyReader.ChildrenIterator it = taxoReader.getChildren(ord);
				while (it.next() != TaxonomyReader.INVALID_ORDINAL) {
					count++;
				}
				return count;
			}
			catch (IOException e) {
				throw new RuntimeException("Failed to count children for dimension ordinal " + ord, e);
			}
		});
	}

	public ZuliaServiceOuterClass.GetFieldNamesResponse getFields() {

		ZuliaServiceOuterClass.GetFieldNamesResponse.Builder builder = ZuliaServiceOuterClass.GetFieldNamesResponse.newBuilder();

		for (LeafReaderContext subReaderContext : indexReader.leaves()) {
			FieldInfos fieldInfos = subReaderContext.reader().getFieldInfos();
			for (FieldInfo fi : fieldInfos) {

				if (fi.getPointDimensionCount() > 0) {

					int matchedLength = 0;
					if (fi.name.endsWith(ZuliaFieldConstants.NUMERIC_INT_SUFFIX)) {
						matchedLength = ZuliaFieldConstants.NUMERIC_INT_SUFFIX.length();
					}
					else if (fi.name.endsWith(ZuliaFieldConstants.NUMERIC_LONG_SUFFIX)) {
						matchedLength = ZuliaFieldConstants.NUMERIC_LONG_SUFFIX.length();
					}
					else if (fi.name.endsWith(ZuliaFieldConstants.NUMERIC_FLOAT_SUFFIX)) {
						matchedLength = ZuliaFieldConstants.NUMERIC_FLOAT_SUFFIX.length();
					}
					else if (fi.name.endsWith(ZuliaFieldConstants.NUMERIC_DOUBLE_SUFFIX)) {
						matchedLength = ZuliaFieldConstants.NUMERIC_DOUBLE_SUFFIX.length();
					}
					else if (fi.name.endsWith(ZuliaFieldConstants.BOOL_SUFFIX)) {
						matchedLength = ZuliaFieldConstants.BOOL_SUFFIX.length();
					}
					else if (fi.name.endsWith(ZuliaFieldConstants.DATE_SUFFIX)) {
						matchedLength = ZuliaFieldConstants.DATE_SUFFIX.length();
					}
					else if (fi.name.endsWith(ZuliaFieldConstants.GEO_POINT_SUFFIX)) {
						matchedLength = ZuliaFieldConstants.GEO_POINT_SUFFIX.length();
					}
					builder.addFieldName(fi.name.substring(0, fi.name.length() - matchedLength));

				}
				else {
					builder.addFieldName(fi.name);
				}
			}
		}

		return builder.build();

	}

	public int numDocs() {
		return indexReader.numDocs();
	}

	/** Drops cached query results and applies the current cache settings, so a changed mapping or cache bound takes effect without a reader reopen. */
	public void clearQueryCaches() {
		AsyncCache<@NotNull QueryCacheKey, ZuliaQuery.ShardQueryResponse> previous = queryResultCache;
		buildQueryResultCache();
		previous.synchronous().invalidateAll();
		pinnedQueryResultCache.synchronous().invalidateAll();
		// the pinned cache has no removal listener, so its counter is reset here
		pinnedQueryResultCacheSize.set(0);
	}

	public ZuliaQuery.ShardQueryResponse queryShard(ShardQuery shardQuery) throws Exception {

		QueryCacheKey queryCacheKey = shardQuery.getQueryCacheKey(); //null when don't cache is set
		if (queryCacheKey != null) {

			// Check if the search is existing in the pinned cache, so we can indicate it is cached. Otherwise, compute it in the cache so multiple identical requests are deduplicated
			CompletableFuture<ZuliaQuery.ShardQueryResponse> pinnedCacheHit = pinnedQueryResultCache.getIfPresent(queryCacheKey);
			if (pinnedCacheHit != null) {
				return pinnedCacheHit.get().toBuilder().setCached(true).setPinned(true).build();
			}

			if (queryCacheKey.isPinned()) {
				// a pinned search is kept whatever its size, since pinning it was an explicit choice
				return getFromAsyncCache(pinnedQueryResultCache, queryCacheKey, shardQuery, pinnedQueryResultCacheSize, Long.MAX_VALUE);
			}

			int segmentQueryCacheMaxAmount = indexConfig.getIndexSettings().getShardQueryCacheMaxAmount();
			boolean useCache = (segmentQueryCacheMaxAmount >= shardQuery.getAmount());
			if (useCache) {
				// one snapshot of the cache and its counter, since a settings update can replace both while this request runs
				AsyncCache<@NotNull QueryCacheKey, ZuliaQuery.ShardQueryResponse> cache = queryResultCache;
				AtomicLong cacheSize = queryResultCacheSize;

				// Check if the search is existing, so we can indicate it is cached. Otherwise, compute it in the cache so multiple identical requests are deduplicated
				CompletableFuture<ZuliaQuery.ShardQueryResponse> cachedResult = cache.getIfPresent(queryCacheKey);
				if (cachedResult != null) {
					return cachedResult.get().toBuilder().setCached(true).build();
				}

				return getFromAsyncCache(cache, queryCacheKey, shardQuery, cacheSize, maxCachedResponseBytes);
			}
		}

		return getShardQueryResponse(shardQuery);

	}

	private ZuliaQuery.ShardQueryResponse getFromAsyncCache(AsyncCache<@NotNull QueryCacheKey, ZuliaQuery.ShardQueryResponse> cache,
			QueryCacheKey queryCacheKey, ShardQuery shardQuery, AtomicLong cacheSize, long maxCachedBytes) throws Exception {
		try {
			CompletableFuture<ZuliaQuery.ShardQueryResponse> future = cache.get(queryCacheKey, _ -> {
				try {
					ZuliaQuery.ShardQueryResponse shardQueryResponse = getShardQueryResponse(shardQuery);
					if (cacheSize != null) {
						cacheSize.getAndAdd(shardQueryResponse.getSerializedSize());
					}
					return shardQueryResponse;
				}
				catch (Exception e) {
					throw new WrappedCheckedException(e);
				}
			});
			ZuliaQuery.ShardQueryResponse shardQueryResponse = future.get();
			if (shardQueryResponse.getSerializedSize() > maxCachedBytes) {
				// computed inside the cache so identical concurrent searches share one run, then dropped so one large response cannot crowd out the rest
				cache.synchronous().invalidate(queryCacheKey);
			}
			return shardQueryResponse;
		}
		catch (ExecutionException e) {
			Throwable cause = e.getCause();
			if (cause instanceof WrappedCheckedException wce) {
				throw wce.getCause();
			}
			if (cause instanceof Exception ex) {
				throw ex;
			}
			throw new RuntimeException(cause);
		}
	}

	private ZuliaQuery.ShardQueryResponse getShardQueryResponse(ShardQuery shardQuery) throws Exception {

		int concurrency = shardQuery.getConcurrency();
		if (concurrency == 0) {
			concurrency = 1;
		}

		try (VirtualThreadPerTaskTaskExecutor searchExecutor = new SemaphoreLimitedVirtualPool(concurrency)) {
			IndexSearcher indexSearcher = new IndexSearcher(indexReader, searchExecutor);
			return getShardQueryResponse(shardQuery, indexSearcher, concurrency);
		}
	}

	private ZuliaQuery.ShardQueryResponse getShardQueryResponse(ShardQuery shardQuery, IndexSearcher indexSearcher, int aggregationConcurrency)
			throws Exception {

		PerFieldSimilarityWrapper similarity = getSimilarity(shardQuery.getSimilarityOverrideMap());

		//similarity is only set query time, indexing time all these similarities are the same
		indexSearcher.setSimilarity(similarity);

		if (shardQuery.isDebug()) {
			String labelInfo = shardQuery.getSearchLabel().isEmpty() ? "" : " with label " + shardQuery.getSearchLabel();
			LOG.info("Lucene Query id {}{} for index {}:s{}: {}", shardQuery.getSearchId(), labelInfo, indexName, shardNumber, shardQuery.getQuery());
			LOG.info("Rewritten Query id {}{} for index {}:s{}: {}", shardQuery.getSearchId(), labelInfo, indexName, shardNumber,
					indexSearcher.rewrite(shardQuery.getQuery()));
		}

		int hasMoreAmount = shardQuery.getAmount() + 1;

		CollectorManager<?, ? extends TopDocs> collectorManager;

		boolean sorting = (shardQuery.getSortRequest() != null) && !shardQuery.getSortRequest().getFieldSortList().isEmpty();

		List<SortMeta> sortMetas = new ArrayList<>();

		boolean sortingWithScores = false;

		FieldDoc after = shardQuery.getAfter(shardNumber);
		if (sorting) {
			Sort sort = buildSortFromSortRequest(shardQuery.getSortRequest());
			sortingWithScores = sort.needsScores();
			collectorManager = new TopFieldCollectorManager(sort, hasMoreAmount, after, Integer.MAX_VALUE);

			for (ZuliaQuery.FieldSort fieldSort : shardQuery.getSortRequest().getFieldSortList()) {
				String sf = fieldSort.getSortField();
				if (GeoDistUtil.isGeoDist(sf)) {
					sortMetas.add(new SortMeta(sf, FieldType.GEO_POINT));
				}
				else {
					SortFieldInfo sortFieldInfo = indexConfig.getSortFieldInfo(sf);
					sortMetas.add(new SortMeta(sf, sortFieldInfo != null ? sortFieldInfo.getFieldType() : null));
				}
			}
		}
		else {
			collectorManager = new TopScoreDocCollectorManager(hasMoreAmount, after, Integer.MAX_VALUE);
		}

		ZuliaQuery.ShardQueryResponse.Builder shardQueryReponseBuilder = ZuliaQuery.ShardQueryResponse.newBuilder();

		ZuliaQuery.FacetRequest facetRequest = shardQuery.getFacetRequest();

		List<ZuliaQuery.CountRequest> countRequestList = facetRequest.getCountRequestList();
		List<ZuliaQuery.StatRequest> statRequestList = facetRequest.getStatRequestList();

		boolean hasFacetRequests = !countRequestList.isEmpty();
		boolean hasStatRequests = !statRequestList.isEmpty();
		boolean hasAggregations = hasFacetRequests || hasStatRequests;
		boolean hasDistinctCountRequests = !shardQuery.getDistinctCountRequests().isEmpty();

		TopDocs topDocs;
		if (hasAggregations || hasDistinctCountRequests) {
			List<CollectorManager<?, ?>> managers = new ArrayList<>();
			managers.add(collectorManager);
			if (hasAggregations) {
				managers.add(new FacetsCollectorManager());
			}
			int distinctCountOffset = managers.size();
			List<DistinctCountCollectorManager> distinctCountManagers = List.of();
			if (hasDistinctCountRequests) {
				distinctCountManagers = getDistinctCountManagers(shardQuery.getDistinctCountRequests());
				managers.addAll(distinctCountManagers);
			}

			Object[] results = indexSearcher.search(shardQuery.getQuery(), new MultiCollectorManager(managers.toArray(new CollectorManager<?, ?>[0])));
			topDocs = (TopDocs) results[0];
			int shardTotalHits = (int) topDocs.totalHits.value();

			if (hasAggregations) {
				if (shardTotalHits == 0) {
					addEmptyFacetAndStatGroups(shardQueryReponseBuilder, countRequestList, statRequestList);
				}
				else {
					List<ZuliaQuery.CountRequest> filteredCountRequests = countRequestList.stream().filter(cr -> {
						long shardLimit = cr.getMaxShardHitsForFacet() != 0 ? cr.getMaxShardHitsForFacet() : cr.getMaxTotalHitsForFacet();
						return shardLimit == 0 || shardTotalHits <= shardLimit;
					}).toList();
					List<ZuliaQuery.StatRequest> filteredStatRequests = statRequestList.stream().filter(sr -> {
						long shardLimit = sr.getMaxShardHitsForFacet() != 0 ? sr.getMaxShardHitsForFacet() : sr.getMaxTotalHitsForFacet();
						return shardLimit == 0 || shardTotalHits <= shardLimit;
					}).toList();

					if (!filteredCountRequests.isEmpty() || !filteredStatRequests.isEmpty()) {
						FacetsCollector facetsCollector = (FacetsCollector) results[1];
						handleAggregations(shardQueryReponseBuilder, filteredStatRequests, filteredCountRequests, facetsCollector, aggregationConcurrency,
								shardQuery.isDebug(), shardQuery.getSearchId());
					}
				}
			}

			if (hasDistinctCountRequests) {
				for (int i = 0; i < distinctCountManagers.size(); i++) {
					DistinctValueCollector collected = (DistinctValueCollector) results[distinctCountOffset + i];
					shardQueryReponseBuilder.addDistinctCount(
							distinctCountManagers.get(i).toInternal(collected, indexReader.leaves(), indexSearcher.getTaskExecutor()));
				}
			}
		}
		else {
			topDocs = indexSearcher.search(shardQuery.getQuery(), collectorManager);
		}

		ScoreDoc[] results = topDocs.scoreDocs;
		if (sortingWithScores) {
			TopFieldCollector.populateScores(topDocs.scoreDocs, indexSearcher, shardQuery.getQuery());
		}

		//TODO is there a way to avoid this cast?  should we support more total hits than an int
		int totalHits = (int) topDocs.totalHits.value();

		shardQueryReponseBuilder.setTotalHits(totalHits);

		boolean moreAvailable = (results.length == hasMoreAmount);

		int numResults = Math.min(results.length, shardQuery.getAmount());

		List<ZuliaHighlighter> highlighterList = getHighlighterList(shardQuery.getHighlightList(), shardQuery.getQuery());

		List<AnalysisHandler> analysisHandlerList = getAnalysisHandlerList(shardQuery.getAnalysisRequestList());

		DocumentScoredDocLeafHandler documentScoredDocLeafHandler = new DocumentScoredDocLeafHandler(indexName, shardNumber, shardQuery.getResultFetchType(),
				shardQuery.getFieldsToReturn(), shardQuery.getFieldsToMask(), sortMetas, highlighterList, analysisHandlerList);
		ZuliaQuery.ScoredResult[] scoredResults = documentScoredDocLeafHandler.handle(indexReader, results, ZuliaQuery.ScoredResult[]::new);

		for (int i = 0; i < numResults; i++) {
			shardQueryReponseBuilder.addScoredResult(scoredResults[i]);
		}

		if (moreAvailable) {
			shardQueryReponseBuilder.setNext(scoredResults[numResults]);
		}

		shardQueryReponseBuilder.setIndexName(indexName);
		shardQueryReponseBuilder.setShardNumber(shardNumber);

		if (!analysisHandlerList.isEmpty()) {
			for (AnalysisHandler analysisHandler : analysisHandlerList) {
				ZuliaQuery.AnalysisResult segmentAnalysisResult = analysisHandler.getShardResult();
				if (segmentAnalysisResult != null) {
					shardQueryReponseBuilder.addAnalysisResult(segmentAnalysisResult);
				}
				analysisHandler.close();
			}
		}
		return shardQueryReponseBuilder.build();
	}

	private List<DistinctCountCollectorManager> getDistinctCountManagers(List<ZuliaQuery.DistinctCountRequest> distinctCountRequests) {
		List<DistinctCountCollectorManager> managers = new ArrayList<>(distinctCountRequests.size());
		for (ZuliaQuery.DistinctCountRequest request : distinctCountRequests) {
			SortFieldInfo sortFieldInfo = Objects.requireNonNull(indexConfig.getSortFieldInfo(request.getField()),
					() -> "Distinct count field <" + request.getField() + "> is not sortable in index <" + indexName + ">");
			managers.add(new DistinctCountCollectorManager(request, sortFieldInfo));
		}
		return managers;
	}

	private void handleAggregations(ZuliaQuery.ShardQueryResponse.Builder shardQueryReponseBuilder, List<ZuliaQuery.StatRequest> statRequestList,
			List<ZuliaQuery.CountRequest> countRequestList, FacetsCollector facetsCollector, int aggregrationConcurrency, boolean debug, long searchId)
			throws IOException {

		AggregationHandler aggregationHandler = new AggregationHandler(taxoReader, facetsCollector, statRequestList, countRequestList, indexConfig,
				aggregrationConcurrency, this::getDimensionChildCount, debug, searchId, aggregationSettings);

		for (ZuliaQuery.CountRequest countRequest : countRequestList) {

			ZuliaQuery.Facet facetField = countRequest.getFacetField();
			String label = facetField.getLabel();

			if (!indexConfig.existingFacet(label)) {
				throw new IllegalArgumentException(label + " is not defined as a facetable field");
			}

			ZuliaQuery.FacetGroup.Builder facetGroup;

			int numOfFacets = getFacetCount(countRequest.getShardFacets(), countRequest.getMaxFacets());

			String[] path = getPathForFacetField(facetField);
			facetGroup = aggregationHandler.getTopChildren(numOfFacets, label, path);

			facetGroup.setCountRequest(countRequest);
			shardQueryReponseBuilder.addFacetGroup(facetGroup);

		}

		for (ZuliaQuery.StatRequest statRequest : statRequestList) {

			ZuliaQuery.StatGroupInternal.Builder statGroupBuilder = ZuliaQuery.StatGroupInternal.newBuilder();
			statGroupBuilder.setStatRequest(statRequest);
			ZuliaQuery.Facet facetField = statRequest.getFacetField();
			String label = facetField.getLabel();
			if (!label.isEmpty()) {

				int numOfFacets = getFacetCount(statRequest.getShardFacets(), statRequest.getMaxFacets());

				if (!indexConfig.existingFacet(label)) {
					throw new IllegalArgumentException(label + " is not defined as a facetable field");
				}

				String[] path = getPathForFacetField(facetField);
				List<ZuliaQuery.FacetStatsInternal> topChildren = aggregationHandler.getTopChildren(statRequest.getNumericField(), numOfFacets, label, path);
				statGroupBuilder.addAllFacetStats(topChildren);

			}
			else {
				ZuliaQuery.FacetStatsInternal globalStats = aggregationHandler.getGlobalStatsForNumericField(statRequest.getNumericField());
				statGroupBuilder.setGlobalStats(globalStats);
			}

			shardQueryReponseBuilder.addStatGroup(statGroupBuilder.build());
		}

	}

	private static void addEmptyFacetAndStatGroups(ZuliaQuery.ShardQueryResponse.Builder shardQueryReponseBuilder,
			List<ZuliaQuery.CountRequest> countRequestList, List<ZuliaQuery.StatRequest> statRequestList) {
		for (ZuliaQuery.CountRequest countRequest : countRequestList) {
			shardQueryReponseBuilder.addFacetGroup(ZuliaQuery.FacetGroup.newBuilder().setCountRequest(countRequest));
		}
		for (ZuliaQuery.StatRequest statRequest : statRequestList) {
			ZuliaQuery.StatGroupInternal.Builder statGroupBuilder = ZuliaQuery.StatGroupInternal.newBuilder().setStatRequest(statRequest);
			if (statRequest.getFacetField().getLabel().isEmpty()) {
				statGroupBuilder.setGlobalStats(ZuliaQuery.FacetStatsInternal.getDefaultInstance());
			}
			shardQueryReponseBuilder.addStatGroup(statGroupBuilder);
		}
	}

	private static String @NotNull [] getPathForFacetField(ZuliaQuery.Facet facetField) {
		String[] path;
		if (!facetField.getPathList().isEmpty()) {
			path = facetField.getPathList().toArray(new String[0]);
		}
		else {
			path = EMPTY_STRING_ARRAY;
		}
		return path;
	}

	private List<AnalysisHandler> getAnalysisHandlerList(List<ZuliaQuery.AnalysisRequest> analysisRequests) {
		if (analysisRequests.isEmpty()) {
			return Collections.emptyList();
		}

		List<AnalysisHandler> analysisHandlerList = new ArrayList<>();
		for (ZuliaQuery.AnalysisRequest analysisRequest : analysisRequests) {

			Analyzer analyzer = zuliaPerFieldAnalyzer;

			String analyzerOverride = analysisRequest.getAnalyzerOverride();
			if (!analyzerOverride.isEmpty()) {

				ZuliaIndex.AnalyzerSettings analyzerSettings = indexConfig.getAnalyzerSettingsByName(analyzerOverride);
				if (analyzerSettings != null) {
					analyzer = new ZuliaFieldAnalyzer(analyzerSettings);
				}
				else {
					throw new RuntimeException("Invalid analyzer name " + analyzerOverride);
				}
			}

			AnalysisHandler analysisHandler = new AnalysisHandler(this, analyzer, indexConfig, analysisRequest);
			analysisHandlerList.add(analysisHandler);
		}
		return analysisHandlerList;

	}

	private List<ZuliaHighlighter> getHighlighterList(List<ZuliaQuery.HighlightRequest> highlightRequests, Query q) {

		if (highlightRequests.isEmpty()) {
			return Collections.emptyList();
		}

		List<ZuliaHighlighter> highlighterList = new ArrayList<>();

		for (ZuliaQuery.HighlightRequest highlightRequest : highlightRequests) {

			String indexField = highlightRequest.getField();

			IndexFieldInfo indexFieldInfo = indexConfig.getIndexFieldInfo(indexField);

			if (indexFieldInfo == null) {
				throw new RuntimeException("Cannot highlight non-indexed field " + indexField);
			}

			QueryScorer queryScorer = new QueryScorer(q, highlightRequest.getField());
			queryScorer.setExpandMultiTermQuery(true);
			Fragmenter fragmenter = new SimpleSpanFragmenter(queryScorer, highlightRequest.getFragmentLength());
			SimpleHTMLFormatter simpleHTMLFormatter = new SimpleHTMLFormatter(highlightRequest.getPreTag(), highlightRequest.getPostTag());
			ZuliaHighlighter highlighter = new ZuliaHighlighter(simpleHTMLFormatter, queryScorer, highlightRequest.getField(),
					indexFieldInfo.getStoredFieldName(), highlightRequest.getNumberOfFragments(), zuliaPerFieldAnalyzer);
			highlighter.setTextFragmenter(fragmenter);
			highlighterList.add(highlighter);
		}
		return highlighterList;
	}

	private PerFieldSimilarityWrapper getSimilarity(Map<String, ZuliaBase.Similarity> similarityOverrideMap) {
		return new PerFieldSimilarityWrapper() {
			@Override
			public org.apache.lucene.search.similarities.Similarity get(String name) {
				IndexFieldInfo indexFieldInfo = indexConfig.getIndexFieldInfo(name);

				if (indexFieldInfo == null) {
					//when field does not exist and a sort is used this case is hit
					return new ConstantSimilarity();
				}

				ZuliaIndex.IndexAs indexAs = indexFieldInfo.getIndexAs();
				AnalyzerSettings analyzerSettings = indexAs != null ? indexConfig.getAnalyzerSettingsByName(indexAs.getAnalyzerName()) : null;
				ZuliaBase.Similarity similarity = ZuliaBase.Similarity.BM25;
				if (analyzerSettings != null) {
					similarity = analyzerSettings.getSimilarity();
				}

				if (similarityOverrideMap != null) {
					ZuliaBase.Similarity fieldSimilarityOverride = similarityOverrideMap.get(name);
					if (fieldSimilarityOverride != null) {
						similarity = fieldSimilarityOverride;
					}
				}

				if (ZuliaBase.Similarity.TFIDF.equals(similarity)) {
					return new ClassicSimilarity();
				}
				else if (ZuliaBase.Similarity.BM25.equals(similarity)) {
					return new BM25Similarity();
				}
				else if (ZuliaBase.Similarity.CONSTANT.equals(similarity)) {
					return new ConstantSimilarity();
				}
				else if (ZuliaBase.Similarity.TF.equals(similarity)) {
					return new TFSimilarity();
				}
				else {
					throw new RuntimeException("Unknown similarity type " + similarity);
				}
			}
		};
	}

	private int getFacetCount(int shardFacets, int maxFacets) {
		int numOfFacets;
		if (indexConfig.getNumberOfShards() > 1) {
			if (shardFacets > 0) {
				numOfFacets = shardFacets;
			}
			else if (shardFacets == 0) {
				numOfFacets = maxFacets * 10;
			}
			else {
				numOfFacets = getTotalFacets();
			}
		}
		else {
			if (maxFacets > 0) {
				numOfFacets = maxFacets;
			}
			else {
				numOfFacets = getTotalFacets();
			}
		}
		return numOfFacets;
	}

	private @NotNull Sort buildSortFromSortRequest(ZuliaQuery.SortRequest sortRequest) throws Exception {
		List<SortField> sortFields = new ArrayList<>();

		for (ZuliaQuery.FieldSort fs : sortRequest.getFieldSortList()) {
			boolean reverse = ZuliaQuery.FieldSort.Direction.DESCENDING.equals(fs.getDirection());

			String sortField = fs.getSortField();

			if (ZuliaFieldConstants.SCORE_FIELD.equals(sortField)) {
				sortFields.add(new SortField(null, SortField.Type.SCORE, !reverse));
				continue;
			}

			if (GeoDistUtil.isGeoDist(sortField)) {
				GeoDistUtil.GeoDistParsed parsed = GeoDistUtil.parseGeoDist(sortField);
				SortFieldInfo geoSortFieldInfo = indexConfig.getSortFieldInfo(parsed.field());
				if (geoSortFieldInfo == null) {
					throw new IllegalArgumentException(
							"Field <" + parsed.field() + "> is not a sortable field. Add .sort() or .sortAs() to the field config to use geodist().");
				}
				if (!FieldTypeUtil.isGeoPointFieldType(geoSortFieldInfo.getFieldType())) {
					throw new IllegalArgumentException("Field <" + parsed.field() + "> is not a GEO_POINT field");
				}
				String internalFieldName = geoSortFieldInfo.getInternalSortFieldName();
				if (reverse) {
					// LatLonDocValuesField.newDistanceSort() doesn't support reverse (BKD competitive iterators),
					// so use GeoDistanceValuesSource for descending distance sort
					GeoDistanceValuesSource dvs = new GeoDistanceValuesSource(internalFieldName, parsed.latitude(), parsed.longitude());
					sortFields.add(dvs.getSortField(true));
				}
				else {
					// Use optimized BKD competitive iterator sort for ascending
					sortFields.add(LatLonDocValuesField.newDistanceSort(internalFieldName, parsed.latitude(), parsed.longitude()));
				}
				continue;
			}

			SortFieldInfo sortFieldInfo = indexConfig.getSortFieldInfo(sortField);

			if (sortFieldInfo == null) {
				throw new IllegalArgumentException("Field " + sortField + " must be sortable");
			}

			FieldType sortFieldType = sortFieldInfo.getFieldType();
			String internalSortFieldName = sortFieldInfo.getInternalSortFieldName();

			if (FieldTypeUtil.isStringFieldType(sortFieldType)) {

				SortedSetSelector.Type sortedSetSelector = SortedSetSelector.Type.MIN;
				if (reverse) {
					sortedSetSelector = SortedSetSelector.Type.MAX;
				}

				SortedSetSortField setSortField = new SortedSetSortField(internalSortFieldName, reverse, sortedSetSelector,
						!fs.getMissingLast() ? SortField.STRING_FIRST : SortField.STRING_LAST);
				sortFields.add(setSortField);
			}
			else {

				SortedNumericSelector.Type sortedNumericSelector = SortedNumericSelector.Type.MIN;
				if (reverse) {
					sortedNumericSelector = SortedNumericSelector.Type.MAX;
				}

				Object missingValue;
				SortField.Type type;
				if (FieldTypeUtil.isStoredAsInt(sortFieldType)) {
					type = SortField.Type.INT;
					missingValue = !fs.getMissingLast() ? Integer.MIN_VALUE : Integer.MAX_VALUE;
				}
				else if (FieldTypeUtil.isStoredAsLong(sortFieldType)) {
					type = SortField.Type.LONG;
					missingValue = !fs.getMissingLast() ? Long.MIN_VALUE : Long.MAX_VALUE;
				}
				else if (FieldTypeUtil.isNumericFloatFieldType(sortFieldType)) {
					type = SortField.Type.FLOAT;
					missingValue = !fs.getMissingLast() ? Float.NEGATIVE_INFINITY : Float.POSITIVE_INFINITY;
				}
				else if (FieldTypeUtil.isNumericDoubleFieldType(sortFieldType)) {
					type = SortField.Type.DOUBLE;
					missingValue = !fs.getMissingLast() ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY;
				}
				else {
					throw new Exception("Invalid numeric sort type " + sortFieldType + " for sort field " + sortField);
				}

				sortFields.add(new SortedNumericSortField(internalSortFieldName, type, reverse, sortedNumericSelector, missingValue));
			}

		}

		return new Sort(sortFields.toArray(new SortField[0]));
	}

	public ZuliaBase.ResultDocument getSourceDocument(String uniqueId, ZuliaQuery.FetchType resultFetchType, List<String> fieldsToReturn,
			List<String> fieldsToMask, boolean realtime) throws Exception {

		ShardQuery shardQuery = ShardQuery.queryById(uniqueId, resultFetchType, fieldsToReturn, fieldsToMask, realtime);

		ZuliaQuery.ShardQueryResponse segmentResponse = this.queryShard(shardQuery);

		List<ZuliaQuery.ScoredResult> scoredResultList = segmentResponse.getScoredResultList();
		if (!scoredResultList.isEmpty()) {
			ZuliaQuery.ScoredResult scoredResult = scoredResultList.getFirst();
			if (scoredResult.hasResultDocument()) {
				return scoredResult.getResultDocument();
			}
		}

		ZuliaBase.ResultDocument.Builder rdBuilder = ZuliaBase.ResultDocument.newBuilder();
		rdBuilder.setUniqueId(uniqueId);
		rdBuilder.setIndexName(indexName);
		return rdBuilder.build();

	}

	public Map<String, ZuliaBase.ResultDocument> getSourceDocuments(List<String> uniqueIds, ZuliaQuery.FetchType resultFetchType, List<String> fieldsToReturn,
			List<String> fieldsToMask, boolean realtime) throws Exception {

		ShardQuery shardQuery = ShardQuery.queryByIds(uniqueIds, resultFetchType, fieldsToReturn, fieldsToMask, realtime);
		ZuliaQuery.ShardQueryResponse segmentResponse = this.queryShard(shardQuery);

		Map<String, ZuliaBase.ResultDocument> results = new HashMap<>(uniqueIds.size());
		for (ZuliaQuery.ScoredResult scoredResult : segmentResponse.getScoredResultList()) {
			if (scoredResult.hasResultDocument()) {
				results.put(scoredResult.getUniqueId(), scoredResult.getResultDocument());
			}
		}
		for (String uniqueId : uniqueIds) {
			if (!results.containsKey(uniqueId)) {
				results.put(uniqueId, ZuliaBase.ResultDocument.newBuilder().setUniqueId(uniqueId).setIndexName(indexName).build());
			}
		}
		return results;
	}

	public int docFreq(String field, String term) throws IOException {
		return indexReader.docFreq(new Term(field, term));
	}

	public ShardTermsHandler getShardTermsHandler() {
		return new ShardTermsHandler(indexReader);
	}

	public int getRefCount() {
		return indexReader.getRefCount();
	}

	public boolean tryIncRef() throws IOException {
		if (indexReader.tryIncRef()) {
			if (taxoReader.tryIncRef()) {
				return true;
			}
			else {
				indexReader.decRef();
			}
		}
		return false;
	}

	public void decRef() throws IOException {
		indexReader.decRef();
		taxoReader.decRef();
	}

	public ShardReader refreshIfNeeded() throws IOException {

		DirectoryReader r = DirectoryReader.openIfChanged(indexReader, segmentOpenExecutor);
		if (r == null) {
			return null;
		}
		else {
			DirectoryTaxonomyReader tr = TaxonomyReader.openIfChanged(taxoReader);
			if (tr == null) {
				taxoReader.incRef();
				tr = taxoReader;
			}

			return new ShardReader(shardNumber, r, tr, indexConfig, zuliaPerFieldAnalyzer, segmentOpenExecutor, aggregationSettings);
		}

	}

	public void streamAllDocs(Consumer<ReIndexContainer> documentConsumer) throws IOException {

		for (LeafReaderContext leaf : indexReader.leaves()) {
			LeafReader leafReader = leaf.reader();

			BinaryDocValues idDocValues = leafReader.getBinaryDocValues(ZuliaFieldConstants.STORED_ID_FIELD);
			BinaryDocValues metaDocValues = leafReader.getBinaryDocValues(ZuliaFieldConstants.STORED_META_FIELD);
			BinaryDocValues fullDocValues = leafReader.getBinaryDocValues(ZuliaFieldConstants.STORED_DOC_FIELD);

			Bits leafLiveDocs = leafReader.getLiveDocs();
			DocIdSetIterator allDocs = DocIdSetIterator.range(leaf.docBase, leaf.docBase + leafReader.maxDoc());

			if (leafLiveDocs != null) {
				allDocs = new FilteredDocIdSetIterator(allDocs) {
					@Override
					protected boolean match(int doc) {
						return leafLiveDocs.get(doc - leaf.docBase);
					}
				};
			}
			int docId;

			while ((docId = allDocs.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
				docId = docId - leaf.docBase;
				idDocValues.advanceExact(docId);

				BytesRef meta = (metaDocValues != null && metaDocValues.advanceExact(docId)) ? metaDocValues.binaryValue() : null;
				BytesRef fullDoc = (fullDocValues != null && fullDocValues.advanceExact(docId)) ? fullDocValues.binaryValue() : null;

				documentConsumer.accept(new ReIndexContainer(idDocValues.binaryValue(), meta, fullDoc));
			}

		}
	}

	public ZuliaBase.ShardCacheStats.Builder getShardCacheStats() {
		ZuliaBase.ShardCacheStats.Builder b = ZuliaBase.ShardCacheStats.newBuilder();
		AsyncCache<@NotNull QueryCacheKey, ZuliaQuery.ShardQueryResponse> cache = queryResultCache;
		b.setGeneralCache(getCacheStats(cache.synchronous(), queryResultCacheSize));
		b.setPinnedCache(getCacheStats(pinnedQueryResultCache.synchronous(), pinnedQueryResultCacheSize));
		return b;
	}

	/**
	 * Cache statistics with the bytes held. A weighted cache reports Caffeine's own weighted size, any other the counter kept beside it.
	 */
	private static ZuliaBase.CacheStats.Builder getCacheStats(Cache<@NotNull QueryCacheKey, ZuliaQuery.ShardQueryResponse> cache, AtomicLong countedSize) {
		CacheStats stats = cache.stats();
		ZuliaBase.CacheStats.Builder b = ZuliaBase.CacheStats.newBuilder();
		b.setEstimatedSize(cache.estimatedSize());
		b.setHitCount(stats.hitCount());
		b.setMissCount(stats.missCount());
		b.setTotalLoadTime(stats.totalLoadTime() / 1_000_000);
		b.setEvictionCount(stats.evictionCount());
		OptionalLong weightedSize = cache.policy().eviction().map(Policy.Eviction::weightedSize).orElse(OptionalLong.empty());
		// the counter is null for a weighted cache, and a stats call can observe a cache swap between reading the two fields
		b.setResultSize(weightedSize.isPresent() ? weightedSize.getAsLong() : countedSize == null ? 0 : countedSize.get());
		return b;
	}
}
