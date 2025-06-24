package io.zulia.server.index;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import io.zulia.ZuliaFieldConstants;
import io.zulia.message.ZuliaBase;
import io.zulia.message.ZuliaIndex;
import io.zulia.message.ZuliaIndex.AnalyzerSettings;
import io.zulia.message.ZuliaIndex.FieldConfig.FieldType;
import io.zulia.message.ZuliaQuery;
import io.zulia.message.ZuliaServiceOuterClass;
import io.zulia.server.analysis.ZuliaPerFieldAnalyzer;
import io.zulia.server.analysis.highlight.ZuliaHighlighter;
import io.zulia.server.analysis.similarity.ConstantSimilarity;
import io.zulia.server.analysis.similarity.TFSimilarity;
import io.zulia.server.config.IndexFieldInfo;
import io.zulia.server.config.ServerIndexConfig;
import io.zulia.server.config.SortFieldInfo;
import io.zulia.server.exceptions.WrappedCheckedException;
import io.zulia.server.field.FieldTypeUtil;
import io.zulia.server.search.QueryCacheKey;
import io.zulia.server.search.ShardQuery;
import io.zulia.server.search.aggregation.AggregationHandler;
import io.zulia.util.pool.SemaphoreLimitedVirtualPool;
import io.zulia.util.pool.VirtualThreadPerTaskTaskExecutor;
import org.apache.lucene.analysis.Analyzer;
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
import java.util.List;
import java.util.Map;
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
	private final Cache<QueryCacheKey, ZuliaQuery.ShardQueryResponse.Builder> queryResultCache;
	private final Cache<QueryCacheKey, ZuliaQuery.ShardQueryResponse.Builder> pinnedQueryResultCache;

	public ShardReader(int shardNumber, DirectoryReader indexReader, DirectoryTaxonomyReader taxoReader, ServerIndexConfig indexConfig,
			ZuliaPerFieldAnalyzer zuliaPerFieldAnalyzer) {
		this.creationTime = System.currentTimeMillis();
		this.shardNumber = shardNumber;
		this.indexReader = indexReader;
		this.taxoReader = taxoReader;
		this.indexConfig = indexConfig;
		this.indexName = indexConfig.getIndexName();
		this.zuliaPerFieldAnalyzer = zuliaPerFieldAnalyzer;
		this.queryResultCache = Caffeine.newBuilder().maximumSize(indexConfig.getIndexSettings().getShardQueryCacheSize()).recordStats().build();
		this.pinnedQueryResultCache = Caffeine.newBuilder().recordStats().build();

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

	public ZuliaQuery.ShardQueryResponse queryShard(ShardQuery shardQuery) throws Exception {

		QueryCacheKey queryCacheKey = shardQuery.getQueryCacheKey(); //null when don't cache is set
		if (queryCacheKey != null) {
			ZuliaQuery.ShardQueryResponse.Builder pinnedCacheHit;

			// Check if the search is existing in the pinned cache, so we can indicate it is cached. Otherwise, compute it in the cache so multiple identical requests are deduplicated
			pinnedCacheHit = pinnedQueryResultCache.getIfPresent(queryCacheKey);
			if (pinnedCacheHit != null) {
				return pinnedCacheHit.setCached(true).setPinned(true).build();
			}

			if (queryCacheKey.isPinned()) {
				return getShardQueryResponseAndCache(queryCacheKey, shardQuery, pinnedQueryResultCache);
			}

			int segmentQueryCacheMaxAmount = indexConfig.getIndexSettings().getShardQueryCacheMaxAmount();
			boolean useCache = (segmentQueryCacheMaxAmount >= shardQuery.getAmount());
			if (useCache) {

				// Check if the search is existing, so we can indicate it is cached. Otherwise, compute it in the cache so multiple identical requests are deduplicated
				ZuliaQuery.ShardQueryResponse.Builder cachedResult = queryResultCache.getIfPresent(queryCacheKey);
				if (cachedResult != null) {
					return cachedResult.setCached(true).build();
				}

				return getShardQueryResponseAndCache(queryCacheKey, shardQuery, queryResultCache);
			}
		}

		return getShardQueryResponseAndCache(shardQuery).build();

	}

	private ZuliaQuery.ShardQueryResponse getShardQueryResponseAndCache(QueryCacheKey queryCacheKey, ShardQuery shardQuery,
			Cache<QueryCacheKey, ZuliaQuery.ShardQueryResponse.Builder> queryResultCache) throws Exception {
		try {
			return queryResultCache.get(queryCacheKey, key -> {
				try {
					return getShardQueryResponseAndCache(shardQuery);
				}
				catch (Exception e) {
					throw new WrappedCheckedException(e);
				}
			}).build();
		}
		catch (WrappedCheckedException e) {
			throw e.getCause();
		}
	}

	private ZuliaQuery.ShardQueryResponse.Builder getShardQueryResponseAndCache(ShardQuery shardQuery) throws Exception {

		int concurrency = shardQuery.getConcurrency();
		if (concurrency == 0) {
			concurrency = 1;
		}

		try (VirtualThreadPerTaskTaskExecutor searchExecutor = new SemaphoreLimitedVirtualPool(concurrency)) {
			IndexSearcher indexSearcher = new IndexSearcher(indexReader, searchExecutor);
			return getShardQueryResponseAndCache(shardQuery, indexSearcher, concurrency);
		}
	}

	private ZuliaQuery.ShardQueryResponse.Builder getShardQueryResponseAndCache(ShardQuery shardQuery, IndexSearcher indexSearcher, int aggregationConcurrency)
			throws Exception {

		PerFieldSimilarityWrapper similarity = getSimilarity(shardQuery.getSimilarityOverrideMap());

		//similarity is only set query time, indexing time all these similarities are the same
		indexSearcher.setSimilarity(similarity);

		if (shardQuery.isDebug()) {
			LOG.info("Lucene Query for index {}:s{}: {}", indexName, shardNumber, shardQuery.getQuery());
			LOG.info("Rewritten Query for index {}:s{}: {}", indexName, shardNumber, indexSearcher.rewrite(shardQuery.getQuery()));
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
				SortFieldInfo sortFieldInfo = indexConfig.getSortFieldInfo(fieldSort.getSortField());
				sortMetas.add(new SortMeta(fieldSort.getSortField(), sortFieldInfo != null ? sortFieldInfo.getFieldType() : null));
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

		TopDocs topDocs;
		if (hasFacetRequests || hasStatRequests) {
			FacetsCollectorManager facetsCollectorManager = new FacetsCollectorManager();
			Object[] results = indexSearcher.search(shardQuery.getQuery(), new MultiCollectorManager(collectorManager, facetsCollectorManager));
			topDocs = (TopDocs) results[0];
			FacetsCollector facetsCollector = (FacetsCollector) results[1];
			handleAggregations(shardQueryReponseBuilder, statRequestList, countRequestList, facetsCollector, aggregationConcurrency);
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
			}
		}
		return shardQueryReponseBuilder;
	}

	private void handleAggregations(ZuliaQuery.ShardQueryResponse.Builder shardQueryReponseBuilder, List<ZuliaQuery.StatRequest> statRequestList,
			List<ZuliaQuery.CountRequest> countRequestList, FacetsCollector facetsCollector, int aggregrationConcurrency) throws IOException {

		AggregationHandler aggregationHandler = new AggregationHandler(taxoReader, facetsCollector, statRequestList, countRequestList, indexConfig,
				aggregrationConcurrency);

		for (ZuliaQuery.CountRequest countRequest : countRequestList) {

			ZuliaQuery.Facet facetField = countRequest.getFacetField();
			String label = facetField.getLabel();

			if (!indexConfig.existingFacet(label)) {
				throw new IllegalArgumentException(label + " is not defined as a facetable field");
			}

			ZuliaQuery.FacetGroup.Builder facetGroup;

			int numOfFacets = getFacetCount(countRequest.getShardFacets(), countRequest.getMaxFacets());

			if (indexConfig.isHierarchicalFacet(label)) {
				facetGroup = aggregationHandler.getTopChildren(numOfFacets, label, facetField.getPathList().toArray(new String[0]));
			}
			else {
				facetGroup = aggregationHandler.getTopChildren(numOfFacets, label);
			}
			facetGroup.setCountRequest(countRequest);
			shardQueryReponseBuilder.addFacetGroup(facetGroup);

		}

		for (ZuliaQuery.StatRequest statRequest : statRequestList) {

			ZuliaQuery.StatGroupInternal.Builder statGroupBuilder = ZuliaQuery.StatGroupInternal.newBuilder();
			statGroupBuilder.setStatRequest(statRequest);
			String label = statRequest.getFacetField().getLabel();
			if (!label.isEmpty()) {

				int numOfFacets = getFacetCount(statRequest.getShardFacets(), statRequest.getMaxFacets());

				if (!indexConfig.existingFacet(label)) {
					throw new IllegalArgumentException(label + " is not defined as a facetable field");
				}

				if (indexConfig.isHierarchicalFacet(label)) {
					List<ZuliaQuery.FacetStatsInternal> topChildren = aggregationHandler.getTopChildren(statRequest.getNumericField(), numOfFacets, label,
							statRequest.getFacetField().getPathList().toArray(new String[0]));
					statGroupBuilder.addAllFacetStats(topChildren);
				}
				else {
					List<ZuliaQuery.FacetStatsInternal> topChildren = aggregationHandler.getTopChildren(statRequest.getNumericField(), numOfFacets, label,
							statRequest.getFacetField().getPathList().toArray(new String[0]));
					statGroupBuilder.addAllFacetStats(topChildren);
				}

			}
			else {
				ZuliaQuery.FacetStatsInternal globalStats = aggregationHandler.getGlobalStatsForNumericField(statRequest.getNumericField());
				statGroupBuilder.setGlobalStats(globalStats);
			}

			shardQueryReponseBuilder.addStatGroup(statGroupBuilder.build());
		}

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
					analyzer = ZuliaPerFieldAnalyzer.getAnalyzerForField(analyzerSettings);
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

				SortedSetSortField setSortField = new SortedSetSortField(internalSortFieldName, reverse, sortedSetSelector);
				setSortField.setMissingValue(!fs.getMissingLast() ? SortField.STRING_FIRST : SortField.STRING_LAST);
				sortFields.add(setSortField);
			}
			else {

				SortedNumericSelector.Type sortedNumericSelector = SortedNumericSelector.Type.MIN;
				if (reverse) {
					sortedNumericSelector = SortedNumericSelector.Type.MAX;
				}

				SortField.Type type;
				if (FieldTypeUtil.isStoredAsInt(sortFieldType)) {
					type = SortField.Type.INT;
				}
				else if (FieldTypeUtil.isStoredAsLong(sortFieldType)) {
					type = SortField.Type.LONG;
				}
				else if (FieldTypeUtil.isNumericFloatFieldType(sortFieldType)) {
					type = SortField.Type.FLOAT;
				}
				else if (FieldTypeUtil.isNumericDoubleFieldType(sortFieldType)) {
					type = SortField.Type.DOUBLE;
				}
				else {
					throw new Exception("Invalid numeric sort type " + sortFieldType + " for sort field " + sortField);
				}

				SortedNumericSortField e = new SortedNumericSortField(internalSortFieldName, type, reverse, sortedNumericSelector);
				if (FieldTypeUtil.isStoredAsInt(sortFieldType)) {
					e.setMissingValue(!fs.getMissingLast() ? Integer.MIN_VALUE : Integer.MAX_VALUE);
				}
				else if (FieldTypeUtil.isStoredAsLong(sortFieldType)) {
					e.setMissingValue(!fs.getMissingLast() ? Long.MIN_VALUE : Long.MAX_VALUE);
				}
				else if (FieldTypeUtil.isNumericFloatFieldType(sortFieldType)) {
					e.setMissingValue(!fs.getMissingLast() ? Float.NEGATIVE_INFINITY : Float.POSITIVE_INFINITY);
				}
				else if (FieldTypeUtil.isNumericDoubleFieldType(sortFieldType)) {
					e.setMissingValue(!fs.getMissingLast() ? Double.NEGATIVE_INFINITY : Double.POSITIVE_INFINITY);
				}

				sortFields.add(e);
			}

		}

		Sort sort = new Sort(sortFields.toArray(new SortField[0]));
		return sort;
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

		DirectoryReader r = DirectoryReader.openIfChanged(indexReader);
		if (r == null) {
			return null;
		}
		else {
			DirectoryTaxonomyReader tr = TaxonomyReader.openIfChanged(taxoReader);
			if (tr == null) {
				taxoReader.incRef();
				tr = taxoReader;
			}

			return new ShardReader(shardNumber, r, tr, indexConfig, zuliaPerFieldAnalyzer);
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
				if (metaDocValues != null) {
					metaDocValues.advanceExact(docId);
				}
				if (fullDocValues != null) {
					fullDocValues.advanceExact(docId);
				}
				BytesRef meta = metaDocValues != null ? metaDocValues.binaryValue() : null;
				BytesRef fullDoc = fullDocValues != null ? fullDocValues.binaryValue() : null;
				documentConsumer.accept(new ReIndexContainer(idDocValues.binaryValue(), meta, fullDoc));
			}

		}
	}

	public ZuliaBase.ShardCacheStats getShardCacheStats() {
		return ZuliaBase.ShardCacheStats.newBuilder().setGeneralCache(getCacheStats(queryResultCache)).setPinnedCache(getCacheStats(pinnedQueryResultCache))
				.build();
	}

	private static ZuliaBase.CacheStats getCacheStats(Cache<QueryCacheKey, ZuliaQuery.ShardQueryResponse.Builder> cache) {
		CacheStats stats = cache.stats();

		return ZuliaBase.CacheStats.newBuilder().setEstimatedSize(cache.estimatedSize()).setHitCount(stats.hitCount()).setMissCount(stats.missCount())
				.setLoadSuccessCount(stats.loadSuccessCount()).setLoadFailureCount(stats.loadFailureCount()).setTotalLoadTime(stats.totalLoadTime())
				.setEvictionCount(stats.evictionCount()).build();
	}
}
