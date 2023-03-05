package io.zulia.server.index;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.protobuf.ByteString;
import io.zulia.ZuliaConstants;
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
import io.zulia.server.config.ServerIndexConfig;
import io.zulia.server.exceptions.WrappedCheckedException;
import io.zulia.server.field.FieldTypeUtil;
import io.zulia.server.search.QueryCacheKey;
import io.zulia.server.search.ShardQuery;
import io.zulia.server.search.TaxonomyStatsHandler;
import io.zulia.server.search.queryparser.ZuliaParser;
import io.zulia.server.util.FieldAndSubFields;
import io.zulia.util.ResultHelper;
import io.zulia.util.ZuliaUtil;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.taxonomy.FastTaxonomyFacetCounts;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.search.highlight.Fragmenter;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.apache.lucene.search.highlight.SimpleSpanFragmenter;
import org.apache.lucene.search.highlight.TextFragment;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.zulia.ZuliaConstants.SORT_SUFFIX;

public class ShardReader implements AutoCloseable {

	private final static Logger LOG = Logger.getLogger(ShardReader.class.getSimpleName());
	private final static Pattern sortedDocValuesMessage = Pattern.compile(
			"unexpected docvalues type NONE for field '(.*)' \\(expected one of \\[SORTED, SORTED_SET\\]\\)\\. Re-index with correct docvalues type.");
	private final static Set<String> fetchSet = Set.of(ZuliaConstants.ID_FIELD, ZuliaConstants.TIMESTAMP_FIELD);
	private final static Set<String> fetchSetWithMeta = Set.of(ZuliaConstants.ID_FIELD, ZuliaConstants.TIMESTAMP_FIELD, ZuliaConstants.STORED_META_FIELD);
	private final static Set<String> fetchSetWithDocument = Set.of(ZuliaConstants.ID_FIELD, ZuliaConstants.TIMESTAMP_FIELD, ZuliaConstants.STORED_META_FIELD,
			ZuliaConstants.STORED_DOC_FIELD);

	private final FacetsConfig facetsConfig;
	private final DirectoryReader indexReader;
	private final DirectoryTaxonomyReader taxoReader;
	private final ServerIndexConfig indexConfig;
	private final String indexName;
	private final int shardNumber;
	private final ZuliaPerFieldAnalyzer zuliaPerFieldAnalyzer;

	private final Cache<QueryCacheKey, ZuliaQuery.ShardQueryResponse.Builder> queryResultCache;
	private final Cache<QueryCacheKey, ZuliaQuery.ShardQueryResponse.Builder> pinnedQueryResultCache;

	public ShardReader(int shardNumber, DirectoryReader indexReader, DirectoryTaxonomyReader taxoReader, FacetsConfig facetsConfig,
			ServerIndexConfig indexConfig, ZuliaPerFieldAnalyzer zuliaPerFieldAnalyzer) {
		this.shardNumber = shardNumber;
		this.indexReader = indexReader;
		this.taxoReader = taxoReader;
		this.facetsConfig = facetsConfig;
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

	public int getTotalFacets() {
		return taxoReader.getSize();
	}

	public ZuliaServiceOuterClass.GetFieldNamesResponse getFields() {

		ZuliaServiceOuterClass.GetFieldNamesResponse.Builder builder = ZuliaServiceOuterClass.GetFieldNamesResponse.newBuilder();

		for (LeafReaderContext subReaderContext : indexReader.leaves()) {
			FieldInfos fieldInfos = subReaderContext.reader().getFieldInfos();
			for (FieldInfo fi : fieldInfos) {
				builder.addFieldName(fi.name);
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
		PerFieldSimilarityWrapper similarity = getSimilarity(shardQuery.getSimilarityOverrideMap());

		IndexSearcher indexSearcher = new IndexSearcher(indexReader);
		StoredFields storedFields = indexSearcher.storedFields();

		//similarity is only set query time, indexing time all these similarities are the same
		indexSearcher.setSimilarity(similarity);

		if (shardQuery.isDebug()) {
			LOG.info("Lucene Query for index <" + indexName + "> segment <" + shardNumber + ">: " + shardQuery.getQuery());
			LOG.info("Rewritten Query for index <" + indexName + "> segment <" + shardNumber + ">: " + indexSearcher.rewrite(shardQuery.getQuery()));
		}

		int hasMoreAmount = shardQuery.getAmount() + 1;

		TopDocsCollector<?> collector;

		boolean sorting = (shardQuery.getSortRequest() != null) && !shardQuery.getSortRequest().getFieldSortList().isEmpty();

		if (sorting) {
			collector = getSortingCollector(shardQuery.getSortRequest(), hasMoreAmount, shardQuery.getAfter(shardNumber));
		}
		else {
			collector = TopScoreDocCollector.create(hasMoreAmount, shardQuery.getAfter(shardNumber), Integer.MAX_VALUE);
		}

		ZuliaQuery.ShardQueryResponse.Builder shardQueryReponseBuilder = ZuliaQuery.ShardQueryResponse.newBuilder();

		try {
			boolean hasFacetRequests = (shardQuery.getFacetRequest() != null) && !shardQuery.getFacetRequest().getCountRequestList().isEmpty();
			boolean hasStatRequests = (shardQuery.getFacetRequest() != null) && !shardQuery.getFacetRequest().getStatRequestList().isEmpty();

			if (hasFacetRequests || hasStatRequests) {
				FacetsCollector facetsCollector = new FacetsCollector();
				indexSearcher.search(shardQuery.getQuery(), MultiCollector.wrap(collector, facetsCollector));

				if (hasFacetRequests) {
					List<ZuliaQuery.FacetGroup> facetGroups = handleFacets(shardQuery.getFacetRequest().getCountRequestList(), facetsCollector);
					shardQueryReponseBuilder.addAllFacetGroup(facetGroups);
				}
				if (hasStatRequests) {
					List<ZuliaQuery.StatGroupInternal> statGroups = handleStats(shardQuery.getFacetRequest().getStatRequestList(), facetsCollector);
					shardQueryReponseBuilder.addAllStatGroup(statGroups);
				}
			}
			else {
				indexSearcher.search(shardQuery.getQuery(), collector);
			}
		}
		catch (IllegalStateException e) {
			Matcher m = sortedDocValuesMessage.matcher(e.getMessage());
			if (m.matches()) {
				String field = m.group(1);
				throw new Exception("Field <" + field + "> must have sortAs defined to be sortable");
			}

			throw e;
		}

		TopDocs topDocs = collector.topDocs();
		ScoreDoc[] results = topDocs.scoreDocs;
		if (sorting && (collector.scoreMode() != ScoreMode.COMPLETE_NO_SCORES)) {
			TopFieldCollector.populateScores(topDocs.scoreDocs, indexSearcher, shardQuery.getQuery());
		}

		int totalHits = collector.getTotalHits();

		shardQueryReponseBuilder.setTotalHits(totalHits);

		boolean moreAvailable = (results.length == hasMoreAmount);

		int numResults = Math.min(results.length, shardQuery.getAmount());

		List<ZuliaHighlighter> highlighterList = getHighlighterList(shardQuery.getHighlightList(), shardQuery.getQuery());

		List<AnalysisHandler> analysisHandlerList = getAnalysisHandlerList(shardQuery.getAnalysisRequestList());

		for (int i = 0; i < numResults; i++) {
			ZuliaQuery.ScoredResult.Builder srBuilder = handleDocResult(storedFields, shardQuery.getSortRequest(), sorting, results, i,
					shardQuery.getResultFetchType(), shardQuery.getFieldsToReturn(), shardQuery.getFieldsToMask(), highlighterList, analysisHandlerList);

			shardQueryReponseBuilder.addScoredResult(srBuilder.build());
		}

		if (moreAvailable) {
			ZuliaQuery.ScoredResult.Builder srBuilder = handleDocResult(storedFields, shardQuery.getSortRequest(), sorting, results, numResults,
					ZuliaQuery.FetchType.NONE, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
			shardQueryReponseBuilder.setNext(srBuilder);
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

	private List<ZuliaQuery.StatGroupInternal> handleStats(List<ZuliaQuery.StatRequest> statRequestList, FacetsCollector facetsCollector) throws IOException {
		List<ZuliaQuery.StatGroupInternal> statGroups = new ArrayList<>();

		TaxonomyStatsHandler facets = new TaxonomyStatsHandler(taxoReader, facetsCollector, statRequestList, indexConfig);

		for (ZuliaQuery.StatRequest statRequest : statRequestList) {

			ZuliaQuery.StatGroupInternal.Builder statGroupBuilder = ZuliaQuery.StatGroupInternal.newBuilder();
			statGroupBuilder.setStatRequest(statRequest);
			String label = statRequest.getFacetField().getLabel();
			if (!label.isEmpty()) {

				int numOfFacets;
				if (indexConfig.getNumberOfShards() > 1) {
					if (statRequest.getShardFacets() > 0) {
						numOfFacets = statRequest.getShardFacets();
					}
					else if (statRequest.getShardFacets() == 0) {
						numOfFacets = statRequest.getMaxFacets() * 10;
					}
					else {
						numOfFacets = getTotalFacets();
					}
				}
				else {
					if (statRequest.getMaxFacets() > 0) {
						numOfFacets = statRequest.getMaxFacets();
					}
					else {
						numOfFacets = getTotalFacets();
					}
				}

				if (indexConfig.isHierarchicalFacet(label)) {
					List<ZuliaQuery.FacetStatsInternal> topChildren = facets.getTopChildren(statRequest.getNumericField(), numOfFacets, label,
							statRequest.getFacetField().getPathList().toArray(new String[0]));
					if (topChildren != null) {
						statGroupBuilder.addAllFacetStats(topChildren);
					}
				}
				else {
					List<ZuliaQuery.FacetStatsInternal> topChildren = facets.getTopChildren(statRequest.getNumericField(), numOfFacets, label,
							statRequest.getFacetField().getPathList().toArray(new String[0]));
					if (topChildren != null) {
						statGroupBuilder.addAllFacetStats(topChildren);
					}
				}

			}
			else {
				ZuliaQuery.FacetStatsInternal globalStats = facets.getGlobalStatsForNumericField(statRequest.getNumericField());
				statGroupBuilder.setGlobalStats(globalStats);
			}

			statGroups.add(statGroupBuilder.build());

		}

		return statGroups;
	}

	private List<AnalysisHandler> getAnalysisHandlerList(List<ZuliaQuery.AnalysisRequest> analysisRequests) throws Exception {
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
					throw new RuntimeException("Invalid analyzer name <" + analyzerOverride + ">");
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

		for (ZuliaQuery.HighlightRequest highlight : highlightRequests) {
			QueryScorer queryScorer = new QueryScorer(q, highlight.getField());
			queryScorer.setExpandMultiTermQuery(true);
			Fragmenter fragmenter = new SimpleSpanFragmenter(queryScorer, highlight.getFragmentLength());
			SimpleHTMLFormatter simpleHTMLFormatter = new SimpleHTMLFormatter(highlight.getPreTag(), highlight.getPostTag());
			ZuliaHighlighter highlighter = new ZuliaHighlighter(simpleHTMLFormatter, queryScorer, highlight);
			highlighter.setTextFragmenter(fragmenter);
			highlighterList.add(highlighter);
		}
		return highlighterList;
	}

	private PerFieldSimilarityWrapper getSimilarity(Map<String, ZuliaBase.Similarity> similarityOverrideMap) {
		return new PerFieldSimilarityWrapper() {
			@Override
			public org.apache.lucene.search.similarities.Similarity get(String name) {

				AnalyzerSettings analyzerSettings = indexConfig.getAnalyzerSettingsForIndexField(name);
				ZuliaBase.Similarity similarity = ZuliaBase.Similarity.BM25;
				if (analyzerSettings != null && analyzerSettings.getSimilarity() != null) {
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
					throw new RuntimeException("Unknown similarity type <" + similarity + ">");
				}
			}
		};
	}

	private List<ZuliaQuery.FacetGroup> handleFacets(List<ZuliaQuery.CountRequest> countRequests, FacetsCollector facetsCollector) throws IOException {
		FastTaxonomyFacetCounts facets = new FastTaxonomyFacetCounts(taxoReader, facetsConfig, facetsCollector);

		List<ZuliaQuery.FacetGroup> facetGroups = new ArrayList<>();

		for (ZuliaQuery.CountRequest countRequest : countRequests) {

			ZuliaQuery.Facet facetField = countRequest.getFacetField();
			String label = facetField.getLabel();

			if (!indexConfig.existingFacet(label)) {
				throw new IllegalArgumentException(label + " is not defined as a facetable field");
			}

			FacetResult facetResult = null;

			try {

				int numOfFacets;
				if (indexConfig.getNumberOfShards() > 1) {
					if (countRequest.getShardFacets() > 0) {
						numOfFacets = countRequest.getShardFacets();
					}
					else if (countRequest.getShardFacets() == 0) {
						numOfFacets = countRequest.getMaxFacets() * 10;
					}
					else {
						numOfFacets = getTotalFacets();
					}
				}
				else {
					if (countRequest.getMaxFacets() > 0) {
						numOfFacets = countRequest.getMaxFacets();
					}
					else {
						numOfFacets = getTotalFacets();
					}
				}

				if (indexConfig.isHierarchicalFacet(label)) {
					facetResult = facets.getTopChildren(numOfFacets, label, facetField.getPathList().toArray(new String[0]));
				}
				else {
					facetResult = facets.getTopChildren(numOfFacets, label);
				}

			}
			catch (UncheckedExecutionException e) {
				Throwable cause = e.getCause();
				if (cause.getMessage().contains(" was not indexed with SortedSetDocValues")) {
					//this is when no data has been indexing into a facet or facet does not exist
				}
				else {
					throw e;
				}
			}
			catch (IllegalArgumentException e) {
				if (e.getMessage().equals("dimension \"" + label + "\" was not indexed")) {
					//this is when no data has been indexing into a facet or facet does not exist
				}
				else {
					throw e;
				}
			}
			ZuliaQuery.FacetGroup.Builder fg = ZuliaQuery.FacetGroup.newBuilder();
			fg.setCountRequest(countRequest);

			if (facetResult != null) {

				for (LabelAndValue subResult : facetResult.labelValues) {
					ZuliaQuery.FacetCount.Builder facetCountBuilder = ZuliaQuery.FacetCount.newBuilder();
					facetCountBuilder.setCount(subResult.value.longValue());
					facetCountBuilder.setFacet(subResult.label);
					fg.addFacetCount(facetCountBuilder);
				}
			}
			facetGroups.add(fg.build());
		}
		return facetGroups;
	}

	private TopDocsCollector<?> getSortingCollector(ZuliaQuery.SortRequest sortRequest, int hasMoreAmount, FieldDoc after) throws Exception {
		List<SortField> sortFields = new ArrayList<>();
		TopDocsCollector<?> collector;
		for (ZuliaQuery.FieldSort fs : sortRequest.getFieldSortList()) {
			boolean reverse = ZuliaQuery.FieldSort.Direction.DESCENDING.equals(fs.getDirection());

			String rewrittenField = ZuliaParser.rewriteLengthFields(fs.getSortField());

			String sortField = fs.getSortField();

			boolean lengthField = !rewrittenField.equals(sortField);

			FieldType sortFieldType = indexConfig.getFieldTypeForSortField(sortField);

			if (!lengthField) {
				if (sortFieldType == null) {
					throw new IllegalArgumentException("Field <" + sortField + "> is not defined as sortable");
				}
			}
			else {
				String fieldName = ZuliaParser.removeLengthBars(sortField);
				FieldType fieldType = indexConfig.getFieldTypeForIndexField(fieldName);
				if (fieldType == null) {
					throw new IllegalArgumentException("Cannot sort on length of indexed field <" + fieldName + "> because no field is indexed with that name");
				}
				if (rewrittenField.startsWith(ZuliaConstants.CHAR_LENGTH_PREFIX) && !FieldTypeUtil.isStringFieldType(fieldType)) {
					throw new IllegalArgumentException("Cannot sort on character length of indexed field <" + fieldName + "> because it is not a string field");
				}
			}

			if (ZuliaConstants.SCORE_FIELD.equals(sortField)) {
				sortFields.add(new SortField(null, SortField.Type.SCORE, !reverse));
			}
			else if (lengthField) {
				SortedNumericSelector.Type sortedNumericSelector = SortedNumericSelector.Type.MIN;
				if (reverse) {
					sortedNumericSelector = SortedNumericSelector.Type.MAX;
				}

				SortedNumericSortField e = new SortedNumericSortField(rewrittenField + SORT_SUFFIX, SortField.Type.INT, reverse, sortedNumericSelector);
				e.setMissingValue(!fs.getMissingLast() ? Integer.MIN_VALUE : Integer.MAX_VALUE);
				sortFields.add(e);
			}
			else if (FieldTypeUtil.isNumericOrDateFieldType(sortFieldType)) {

				SortedNumericSelector.Type sortedNumericSelector = SortedNumericSelector.Type.MIN;
				if (reverse) {
					sortedNumericSelector = SortedNumericSelector.Type.MAX;
				}

				SortField.Type type;
				if (FieldTypeUtil.isNumericIntFieldType(sortFieldType)) {
					type = SortField.Type.INT;
				}
				else if (FieldTypeUtil.isNumericLongFieldType(sortFieldType) || FieldTypeUtil.isDateFieldType(sortFieldType)) {
					type = SortField.Type.LONG;
				}
				else if (FieldTypeUtil.isNumericFloatFieldType(sortFieldType)) {
					type = SortField.Type.FLOAT;
				}
				else if (FieldTypeUtil.isNumericDoubleFieldType(sortFieldType)) {
					type = SortField.Type.DOUBLE;
				}
				else {
					throw new Exception("Invalid numeric sort type <" + sortFieldType + "> for sort field <" + sortField + ">");
				}

				SortedNumericSortField e = new SortedNumericSortField(indexConfig.getSortField(sortField, sortFieldType), type, reverse, sortedNumericSelector);
				if (FieldTypeUtil.isNumericIntFieldType(sortFieldType)) {
					e.setMissingValue(!fs.getMissingLast() ? Integer.MIN_VALUE : Integer.MAX_VALUE);
				}
				else if (FieldTypeUtil.isNumericLongFieldType(sortFieldType) || FieldTypeUtil.isDateFieldType(sortFieldType)) {
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
			else if (FieldTypeUtil.isBooleanFieldType(sortFieldType)) {
				SortedNumericSelector.Type sortedNumericSelector = SortedNumericSelector.Type.MIN;
				if (reverse) {
					sortedNumericSelector = SortedNumericSelector.Type.MAX;
				}
				SortedNumericSortField e = new SortedNumericSortField(indexConfig.getSortField(sortField, sortFieldType), SortField.Type.INT, reverse,
						sortedNumericSelector);
				e.setMissingValue(!fs.getMissingLast() ? Integer.MIN_VALUE : Integer.MAX_VALUE);
				sortFields.add(e);
			}
			else {

				SortedSetSelector.Type sortedSetSelector = SortedSetSelector.Type.MIN;
				if (reverse) {
					sortedSetSelector = SortedSetSelector.Type.MAX;
				}

				SortedSetSortField setSortField = new SortedSetSortField(indexConfig.getSortField(sortField, sortFieldType), reverse, sortedSetSelector);
				setSortField.setMissingValue(!fs.getMissingLast() ? SortField.STRING_FIRST : SortField.STRING_LAST);
				sortFields.add(setSortField);
			}

		}

		Sort sort = new Sort(sortFields.toArray(new SortField[0]));

		collector = TopFieldCollector.create(sort, hasMoreAmount, after, Integer.MAX_VALUE);
		return collector;
	}

	private ZuliaQuery.ScoredResult.Builder handleDocResult(StoredFields storedFields, ZuliaQuery.SortRequest sortRequest, boolean sorting, ScoreDoc[] results,
			int i, ZuliaQuery.FetchType resultFetchType, List<String> fieldsToReturn, List<String> fieldsToMask, List<ZuliaHighlighter> highlighterList,
			List<AnalysisHandler> analysisHandlerList) throws Exception {
		int luceneShardId = results[i].doc;

		Set<String> fieldsToFetch = fetchSet;

		if (ZuliaQuery.FetchType.FULL.equals(resultFetchType)) {
			fieldsToFetch = fetchSetWithDocument;
		}
		else if (ZuliaQuery.FetchType.META.equals(resultFetchType)) {
			fieldsToFetch = fetchSetWithMeta;
		}

		Document d = storedFields.document(luceneShardId, fieldsToFetch);

		IndexableField f = d.getField(ZuliaConstants.TIMESTAMP_FIELD);
		long timestamp = f.numericValue().longValue();

		ZuliaQuery.ScoredResult.Builder srBuilder = ZuliaQuery.ScoredResult.newBuilder();
		String uniqueId = d.get(ZuliaConstants.ID_FIELD);

		if (!highlighterList.isEmpty() && !ZuliaQuery.FetchType.FULL.equals(resultFetchType)) {
			throw new Exception("Highlighting requires a full fetch of the document");
		}

		if (!analysisHandlerList.isEmpty() && !ZuliaQuery.FetchType.FULL.equals(resultFetchType)) {
			throw new Exception("Analysis requires a full fetch of the document");
		}

		if (!ZuliaQuery.FetchType.NONE.equals(resultFetchType)) {
			handleStoredDoc(srBuilder, uniqueId, d, resultFetchType, fieldsToReturn, fieldsToMask, highlighterList, analysisHandlerList);
		}

		srBuilder.setScore(results[i].score);

		srBuilder.setUniqueId(uniqueId);

		srBuilder.setTimestamp(timestamp);

		srBuilder.setLuceneShardId(luceneShardId);
		srBuilder.setShard(shardNumber);
		srBuilder.setIndexName(indexName);
		srBuilder.setResultIndex(i);

		if (sorting) {
			handleSortValues(sortRequest, results[i], srBuilder);
		}
		return srBuilder;
	}

	private void handleStoredDoc(ZuliaQuery.ScoredResult.Builder srBuilder, String uniqueId, Document d, ZuliaQuery.FetchType resultFetchType,
			List<String> fieldsToReturn, List<String> fieldsToMask, List<ZuliaHighlighter> highlighterList, List<AnalysisHandler> analysisHandlerList) {

		ZuliaBase.ResultDocument.Builder rdBuilder = ZuliaBase.ResultDocument.newBuilder();
		rdBuilder.setUniqueId(uniqueId);
		rdBuilder.setIndexName(indexName);

		if (ZuliaQuery.FetchType.FULL.equals(resultFetchType) || ZuliaQuery.FetchType.META.equals(resultFetchType)) {
			BytesRef metaRef = d.getBinaryValue(ZuliaConstants.STORED_META_FIELD);
			if (metaRef != null) {
				rdBuilder.setMetadata(ByteString.copyFrom(metaRef.bytes));
			}
		}

		if (ZuliaQuery.FetchType.FULL.equals(resultFetchType)) {
			BytesRef docRef = d.getBinaryValue(ZuliaConstants.STORED_DOC_FIELD);
			if (docRef != null) {
				rdBuilder.setDocument(ByteString.copyFrom(docRef.bytes));
			}
		}

		ZuliaBase.ResultDocument resultDocument = rdBuilder.build();

		if (!highlighterList.isEmpty() || !analysisHandlerList.isEmpty() || !fieldsToMask.isEmpty() || !fieldsToReturn.isEmpty()) {
			org.bson.Document mongoDoc = ResultHelper.getDocumentFromResultDocument(resultDocument);
			if (mongoDoc != null) {
				if (!highlighterList.isEmpty()) {
					handleHighlight(highlighterList, srBuilder, mongoDoc);
				}
				if (!analysisHandlerList.isEmpty()) {
					AnalysisHandler.handleDocument(mongoDoc, analysisHandlerList, srBuilder);
				}

				resultDocument = filterDocument(resultDocument, fieldsToReturn, fieldsToMask, mongoDoc);
			}
		}

		srBuilder.setResultDocument(resultDocument);
	}

	private void handleSortValues(ZuliaQuery.SortRequest sortRequest, ScoreDoc scoreDoc, ZuliaQuery.ScoredResult.Builder srBuilder) {
		FieldDoc result = (FieldDoc) scoreDoc;

		ZuliaQuery.SortValues.Builder sortValues = ZuliaQuery.SortValues.newBuilder();

		int c = 0;

		for (Object o : result.fields) {
			if (o == null) {
				sortValues.addSortValue(ZuliaQuery.SortValue.newBuilder().setExists(false));
				c++;
				continue;
			}

			ZuliaQuery.FieldSort fieldSort = sortRequest.getFieldSort(c);
			String sortField = fieldSort.getSortField();

			if (ZuliaConstants.SCORE_FIELD.equals(sortField)) {
				sortValues.addSortValue(ZuliaQuery.SortValue.newBuilder().setFloatValue(scoreDoc.score));
				c++;
				continue;
			}

			FieldType fieldTypeForSortField = indexConfig.getFieldTypeForSortField(sortField);

			if (!ZuliaParser.rewriteLengthFields(sortField).equals(sortField)) {
				fieldTypeForSortField = FieldType.NUMERIC_INT;
			}

			ZuliaQuery.SortValue.Builder sortValueBuilder = ZuliaQuery.SortValue.newBuilder().setExists(true);
			if (FieldTypeUtil.isNumericOrDateFieldType(fieldTypeForSortField)) {
				if (FieldTypeUtil.isNumericIntFieldType(fieldTypeForSortField)) {
					sortValueBuilder.setIntegerValue((Integer) o);
				}
				else if (FieldTypeUtil.isNumericLongFieldType(fieldTypeForSortField)) {
					sortValueBuilder.setLongValue((Long) o);
				}
				else if (FieldTypeUtil.isNumericFloatFieldType(fieldTypeForSortField)) {
					sortValueBuilder.setFloatValue((Float) o);
				}
				else if (FieldTypeUtil.isNumericDoubleFieldType(fieldTypeForSortField)) {
					sortValueBuilder.setDoubleValue((Double) o);
				}
				else if (FieldTypeUtil.isDateFieldType(fieldTypeForSortField)) {
					sortValueBuilder.setDateValue((Long) o);
				}
			}
			else if (FieldTypeUtil.isBooleanFieldType(fieldTypeForSortField)) {
				sortValueBuilder.setIntegerValue((Integer) o);
			}
			else {
				BytesRef b = (BytesRef) o;
				sortValueBuilder.setStringValue(b.utf8ToString());
			}
			sortValues.addSortValue(sortValueBuilder);

			c++;
		}
		srBuilder.setSortValues(sortValues);
	}

	private void handleHighlight(List<ZuliaHighlighter> highlighterList, ZuliaQuery.ScoredResult.Builder srBuilder, org.bson.Document doc) {

		for (ZuliaHighlighter highlighter : highlighterList) {
			ZuliaQuery.HighlightRequest highlightRequest = highlighter.getHighlight();
			String indexField = highlightRequest.getField();
			String storedFieldName = indexConfig.getStoredFieldName(indexField);

			if (storedFieldName != null) {
				ZuliaQuery.HighlightResult.Builder highLightResult = ZuliaQuery.HighlightResult.newBuilder();

				highLightResult.setField(storedFieldName);

				Object storeFieldValues = ResultHelper.getValueFromMongoDocument(doc, storedFieldName);

				ZuliaUtil.handleLists(storeFieldValues, (value) -> {
					String content = value.toString();
					TokenStream tokenStream = zuliaPerFieldAnalyzer.tokenStream(indexField, content);

					try {
						TextFragment[] bestTextFragments = highlighter.getBestTextFragments(tokenStream, content, false,
								highlightRequest.getNumberOfFragments());
						for (TextFragment bestTextFragment : bestTextFragments) {
							if (bestTextFragment != null && bestTextFragment.getScore() > 0) {
								highLightResult.addFragments(bestTextFragment.toString());
							}
						}
					}
					catch (Exception e) {
						throw new RuntimeException(e);
					}

				});

				srBuilder.addHighlightResult(highLightResult);
			}

		}

	}

	public ZuliaBase.ResultDocument getSourceDocument(String uniqueId, ZuliaQuery.FetchType resultFetchType, List<String> fieldsToReturn,
			List<String> fieldsToMask) throws Exception {

		ZuliaBase.ResultDocument rd = null;

		ShardQuery shardQuery = ShardQuery.queryById(uniqueId, resultFetchType, fieldsToReturn, fieldsToMask);

		ZuliaQuery.ShardQueryResponse segmentResponse = this.queryShard(shardQuery);

		List<ZuliaQuery.ScoredResult> scoredResultList = segmentResponse.getScoredResultList();
		if (!scoredResultList.isEmpty()) {
			ZuliaQuery.ScoredResult scoredResult = scoredResultList.iterator().next();
			if (scoredResult.hasResultDocument()) {
				rd = scoredResult.getResultDocument();
			}
		}

		if (rd != null) {
			if (!fieldsToMask.isEmpty() || !fieldsToReturn.isEmpty()) {
				org.bson.Document mongoDocument = ResultHelper.getDocumentFromResultDocument(rd);
				if (mongoDocument != null) {
					rd = filterDocument(rd, fieldsToReturn, fieldsToMask, mongoDocument);
				}
			}
			return rd;
		}

		ZuliaBase.ResultDocument.Builder rdBuilder = ZuliaBase.ResultDocument.newBuilder();
		rdBuilder.setUniqueId(uniqueId);
		rdBuilder.setIndexName(indexName);
		return rdBuilder.build();

	}

	private ZuliaBase.ResultDocument filterDocument(ZuliaBase.ResultDocument rd, Collection<String> fieldsToReturn, Collection<String> fieldsToMask,
			org.bson.Document mongoDocument) {

		ZuliaBase.ResultDocument.Builder resultDocBuilder = rd.toBuilder();

		filterDocument(fieldsToReturn, fieldsToMask, mongoDocument);

		ByteString document = ZuliaUtil.mongoDocumentToByteString(mongoDocument);
		resultDocBuilder.setDocument(document);

		return resultDocBuilder.build();

	}

	private void filterDocument(Collection<String> fieldsToReturn, Collection<String> fieldsToMask, org.bson.Document mongoDocument) {

		if (fieldsToReturn.isEmpty() && !fieldsToMask.isEmpty()) {
			FieldAndSubFields fieldsToMaskObj = new FieldAndSubFields(fieldsToMask);
			for (String topLevelField : fieldsToMaskObj.getTopLevelFields()) {
				Map<String, Set<String>> topLevelToChildren = fieldsToMaskObj.getTopLevelToChildren();
				if (!topLevelToChildren.containsKey(topLevelField)) {
					mongoDocument.remove(topLevelField);
				}
				else {
					Object subDoc = mongoDocument.get(topLevelField);
					ZuliaUtil.handleLists(subDoc, subDocItem -> {
						if (subDocItem instanceof org.bson.Document) {

							Collection<String> subFieldsToMask =
									topLevelToChildren.get(topLevelField) != null ? topLevelToChildren.get(topLevelField) : Collections.emptyList();

							filterDocument(Collections.emptyList(), subFieldsToMask, (org.bson.Document) subDocItem);
						}
						else if (subDocItem == null) {

						}
						else {
							//TODO: warn user?
						}
					});
				}
			}
		}
		else if (!fieldsToReturn.isEmpty()) {
			FieldAndSubFields fieldsToReturnObj = new FieldAndSubFields(fieldsToReturn);
			FieldAndSubFields fieldsToMaskObj = new FieldAndSubFields(fieldsToMask);

			Set<String> topLevelFieldsToReturn = fieldsToReturnObj.getTopLevelFields();
			Set<String> topLevelFieldsToMask = fieldsToMaskObj.getTopLevelFields();
			Map<String, Set<String>> topLevelToChildrenToMask = fieldsToMaskObj.getTopLevelToChildren();
			Map<String, Set<String>> topLevelToChildrenToReturn = fieldsToReturnObj.getTopLevelToChildren();

			ArrayList<String> allDocumentKeys = new ArrayList<>(mongoDocument.keySet());
			for (String topLevelField : allDocumentKeys) {

				if ((!topLevelFieldsToReturn.contains(topLevelField) && !topLevelToChildrenToMask.containsKey(topLevelField))
						|| topLevelFieldsToMask.contains(topLevelField) && !topLevelToChildrenToMask.containsKey(topLevelField)) {
					mongoDocument.remove(topLevelField);
				}

				if (topLevelToChildrenToReturn.containsKey(topLevelField) || topLevelToChildrenToMask.containsKey(topLevelField)) {
					Object subDoc = mongoDocument.get(topLevelField);
					ZuliaUtil.handleLists(subDoc, subDocItem -> {
						if (subDocItem instanceof org.bson.Document) {

							Collection<String> subFieldsToReturn = topLevelToChildrenToReturn.get(topLevelField) != null ?
									topLevelToChildrenToReturn.get(topLevelField) :
									Collections.emptyList();

							Collection<String> subFieldsToMask =
									topLevelToChildrenToMask.get(topLevelField) != null ? topLevelToChildrenToMask.get(topLevelField) : Collections.emptyList();

							filterDocument(subFieldsToReturn, subFieldsToMask, (org.bson.Document) subDocItem);
						}
						else if (subDocItem == null) {

						}
						else {
							//TODO: warn user?
						}
					});

				}
			}
		}

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

			return new ShardReader(shardNumber, r, tr, facetsConfig, indexConfig, zuliaPerFieldAnalyzer);
		}

	}

	public void streamAllDocs(Consumer<Document> documentConsumer) throws IOException {

		StoredFields storedFields = indexReader.storedFields();
		for (LeafReaderContext leaf : indexReader.leaves()) {
			Bits leafLiveDocs = leaf.reader().getLiveDocs();
			DocIdSetIterator allDocs = DocIdSetIterator.range(leaf.docBase, leaf.docBase + leaf.reader().maxDoc());
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
				Document d = storedFields.document(docId);
				documentConsumer.accept(d);
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
