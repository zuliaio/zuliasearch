package io.zulia.server.index;

import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.protobuf.ByteString;
import io.zulia.ZuliaConstants;
import io.zulia.message.ZuliaBase;
import io.zulia.message.ZuliaIndex;
import io.zulia.message.ZuliaQuery;
import io.zulia.message.ZuliaServiceOuterClass;
import io.zulia.server.analysis.ZuliaPerFieldAnalyzer;
import io.zulia.server.analysis.highlight.ZuliaHighlighter;
import io.zulia.server.analysis.similarity.ConstantSimilarity;
import io.zulia.server.analysis.similarity.TFSimilarity;
import io.zulia.server.config.ServerIndexConfig;
import io.zulia.server.index.field.FieldTypeUtil;
import io.zulia.server.search.QueryCacheKey;
import io.zulia.server.search.QueryResultCache;
import io.zulia.server.util.FieldAndSubFields;
import io.zulia.util.ResultHelper;
import io.zulia.util.ZuliaUtil;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
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
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ShardReader implements AutoCloseable {

	private final static Logger LOG = Logger.getLogger(ShardReader.class.getSimpleName());

	private final static Pattern sortedDocValuesMessage = Pattern.compile(
			"unexpected docvalues type NONE for field '(.*)' \\(expected one of \\[SORTED, SORTED_SET\\]\\)\\. Use UninvertingReader or index with docvalues\\.");

	private final static Set<String> fetchSet = Collections
			.unmodifiableSet(new HashSet<>(Arrays.asList(ZuliaConstants.ID_FIELD, ZuliaConstants.TIMESTAMP_FIELD)));

	private final static Set<String> fetchSetWithMeta = Collections
			.unmodifiableSet(new HashSet<>(Arrays.asList(ZuliaConstants.ID_FIELD, ZuliaConstants.TIMESTAMP_FIELD, ZuliaConstants.STORED_META_FIELD)));

	private final static Set<String> fetchSetWithDocument = Collections.unmodifiableSet(new HashSet<>(
			Arrays.asList(ZuliaConstants.ID_FIELD, ZuliaConstants.TIMESTAMP_FIELD, ZuliaConstants.STORED_META_FIELD, ZuliaConstants.STORED_DOC_FIELD)));

	private final FacetsConfig facetsConfig;
	private final DirectoryReader indexReader;
	private final DirectoryTaxonomyReader taxoReader;
	private final ServerIndexConfig indexConfig;
	private final String indexName;
	private final int shardNumber;
	private final int segmentQueryCacheMaxAmount;
	private final QueryResultCache queryResultCache;
	private final ZuliaPerFieldAnalyzer zuliaPerFieldAnalyzer;

	public ShardReader(int shardNumber, DirectoryReader indexReader, DirectoryTaxonomyReader taxoReader, FacetsConfig facetsConfig,
			ServerIndexConfig indexConfig, ZuliaPerFieldAnalyzer zuliaPerFieldAnalyzer) {
		this.shardNumber = shardNumber;
		this.indexReader = indexReader;
		this.taxoReader = taxoReader;
		this.facetsConfig = facetsConfig;
		this.indexConfig = indexConfig;
		this.indexName = indexConfig.getIndexName();
		this.zuliaPerFieldAnalyzer = zuliaPerFieldAnalyzer;

		segmentQueryCacheMaxAmount = indexConfig.getIndexSettings().getShardQueryCacheMaxAmount();

		int segmentQueryCacheSize = indexConfig.getIndexSettings().getShardQueryCacheSize();
		if ((segmentQueryCacheSize > 0)) {
			this.queryResultCache = new QueryResultCache(segmentQueryCacheSize, 8);
		}
		else {
			this.queryResultCache = null;
		}

	}

	@Override
	public void close() throws Exception {
		indexReader.close();
		taxoReader.close();
	}

	public Facets getFacets(FacetsCollector facetsCollector) throws IOException {
		return new FastTaxonomyFacetCounts(taxoReader, facetsConfig, facetsCollector);
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

	public ZuliaQuery.ShardQueryResponse queryShard(Query query, Map<String, ZuliaBase.Similarity> similarityOverrideMap, int amount, FieldDoc after,
			ZuliaQuery.FacetRequest facetRequest, ZuliaQuery.SortRequest sortRequest, QueryCacheKey queryCacheKey, ZuliaQuery.FetchType resultFetchType,
			List<String> fieldsToReturn, List<String> fieldsToMask, List<ZuliaQuery.HighlightRequest> highlightList,
			List<ZuliaQuery.AnalysisRequest> analysisRequestList, boolean debug) throws Exception {

		QueryResultCache qrc = queryResultCache;

		boolean useCache = (qrc != null) && ((segmentQueryCacheMaxAmount <= 0) || (segmentQueryCacheMaxAmount >= amount)) && queryCacheKey != null;
		if (useCache) {
			ZuliaQuery.ShardQueryResponse cacheShardResponse = qrc.getCachedShardQueryResponse(queryCacheKey);
			if (cacheShardResponse != null) {
				LOG.info("Returning results from cache for query <" + query + "> from index <" + indexName + "> with shard number <" + shardNumber + ">");
				return cacheShardResponse;
			}
		}

		PerFieldSimilarityWrapper similarity = getSimilarity(similarityOverrideMap);

		IndexSearcher indexSearcher = new IndexSearcher(indexReader);

		//similarity is only set query time, indexing time all these similarities are the same
		indexSearcher.setSimilarity(similarity);

		if (debug) {
			LOG.info("Lucene Query for index <" + indexName + "> segment <" + shardNumber + ">: " + query);
			LOG.info("Rewritten Query for index <" + indexName + "> segment <" + shardNumber + ">: " + indexSearcher.rewrite(query));
		}

		int hasMoreAmount = amount + 1;

		TopDocsCollector<?> collector;

		boolean sorting = (sortRequest != null) && !sortRequest.getFieldSortList().isEmpty();
		if (sorting) {

			collector = getSortingCollector(sortRequest, hasMoreAmount, after);
		}
		else {
			collector = TopScoreDocCollector.create(hasMoreAmount, after, Integer.MAX_VALUE);
		}

		ZuliaQuery.ShardQueryResponse.Builder shardQueryReponseBuilder = ZuliaQuery.ShardQueryResponse.newBuilder();

		try {
			if ((facetRequest != null) && !facetRequest.getCountRequestList().isEmpty()) {

				searchWithFacets(facetRequest, query, indexSearcher, collector, shardQueryReponseBuilder);

			}
			else {
				indexSearcher.search(query, collector);
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

		ScoreDoc[] results = collector.topDocs().scoreDocs;

		int totalHits = collector.getTotalHits();

		shardQueryReponseBuilder.setTotalHits(totalHits);

		boolean moreAvailable = (results.length == hasMoreAmount);

		int numResults = Math.min(results.length, amount);

		List<ZuliaHighlighter> highlighterList = getHighlighterList(highlightList, query);

		List<AnalysisHandler> analysisHandlerList = getAnalysisHandlerList(analysisRequestList);

		for (int i = 0; i < numResults; i++) {
			ZuliaQuery.ScoredResult.Builder srBuilder = handleDocResult(indexSearcher, sortRequest, sorting, results, i, resultFetchType, fieldsToReturn,
					fieldsToMask, highlighterList, analysisHandlerList);

			shardQueryReponseBuilder.addScoredResult(srBuilder.build());
		}

		if (moreAvailable) {
			ZuliaQuery.ScoredResult.Builder srBuilder = handleDocResult(indexSearcher, sortRequest, sorting, results, numResults, ZuliaQuery.FetchType.NONE,
					Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
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

		ZuliaQuery.ShardQueryResponse segmentResponse = shardQueryReponseBuilder.build();
		if (useCache) {
			qrc.storeInCache(queryCacheKey, segmentResponse);
		}
		return segmentResponse;

	}

	private List<AnalysisHandler> getAnalysisHandlerList(List<ZuliaQuery.AnalysisRequest> analysisRequests) throws Exception {
		if (analysisRequests.isEmpty()) {
			return Collections.emptyList();
		}

		List<AnalysisHandler> analysisHandlerList = new ArrayList<>();
		for (ZuliaQuery.AnalysisRequest analysisRequest : analysisRequests) {

			Analyzer analyzer = zuliaPerFieldAnalyzer;

			String analyzerOverride = analysisRequest.getAnalyzerOverride();
			if (analyzerOverride != null && !analyzerOverride.isEmpty()) {
				String analyzerName = analyzerOverride;

				ZuliaIndex.AnalyzerSettings analyzerSettings = indexConfig.getAnalyzerSettingsByName(analyzerName);
				if (analyzerSettings != null) {
					analyzer = ZuliaPerFieldAnalyzer.getAnalyzerForField(analyzerSettings);
				}
				else {
					throw new RuntimeException("Invalid analyzer name <" + analyzerName + ">");
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

				ZuliaIndex.AnalyzerSettings analyzerSettings = indexConfig.getAnalyzerSettingsForIndexField(name);
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

	private void searchWithFacets(ZuliaQuery.FacetRequest facetRequest, Query q, IndexSearcher indexSearcher, TopDocsCollector<?> collector,
			ZuliaQuery.ShardQueryResponse.Builder segmentReponseBuilder) throws Exception {
		FacetsCollector facetsCollector = new FacetsCollector();
		indexSearcher.search(q, MultiCollector.wrap(collector, facetsCollector));

		Facets facets = getFacets(facetsCollector);

		for (ZuliaQuery.CountRequest countRequest : facetRequest.getCountRequestList()) {

			String label = countRequest.getFacetField().getLabel();

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

				facetResult = facets.getTopChildren(numOfFacets, label);
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
			segmentReponseBuilder.addFacetGroup(fg);
		}
	}

	private TopDocsCollector<?> getSortingCollector(ZuliaQuery.SortRequest sortRequest, int hasMoreAmount, FieldDoc after) throws Exception {
		List<SortField> sortFields = new ArrayList<>();
		TopDocsCollector<?> collector;
		for (ZuliaQuery.FieldSort fs : sortRequest.getFieldSortList()) {
			boolean reverse = ZuliaQuery.FieldSort.Direction.DESCENDING.equals(fs.getDirection());

			String sortField = fs.getSortField();
			ZuliaIndex.FieldConfig.FieldType sortFieldType = indexConfig.getFieldTypeForSortField(sortField);

			if (FieldTypeUtil.isNumericOrDateFieldType(sortFieldType)) {

				SortedNumericSelector.Type sortedNumericSelector = SortedNumericSelector.Type.MIN;
				if (reverse) {
					sortedNumericSelector = SortedNumericSelector.Type.MAX;
				}

				SortField.Type type;
				if (FieldTypeUtil.isNumericIntFieldType(sortFieldType)) {
					type = SortField.Type.INT;
				}
				else if (FieldTypeUtil.isNumericLongFieldType(sortFieldType)) {
					type = SortField.Type.LONG;
				}
				else if (FieldTypeUtil.isNumericFloatFieldType(sortFieldType)) {
					type = SortField.Type.FLOAT;
				}
				else if (FieldTypeUtil.isNumericDoubleFieldType(sortFieldType)) {
					type = SortField.Type.DOUBLE;
				}
				else if (FieldTypeUtil.isDateFieldType(sortFieldType)) {
					type = SortField.Type.LONG;
				}
				else {
					throw new Exception("Invalid numeric sort type <" + sortFieldType + "> for sort field <" + sortField + ">");
				}
				sortFields.add(new SortedNumericSortField(sortField, type, reverse, sortedNumericSelector));
			}
			else {

				SortedSetSelector.Type sortedSetSelector = SortedSetSelector.Type.MIN;
				if (reverse) {
					sortedSetSelector = SortedSetSelector.Type.MAX;
				}

				sortFields.add(new SortedSetSortField(sortField, reverse, sortedSetSelector));
			}

		}
		Sort sort = new Sort();
		sort.setSort(sortFields.toArray(new SortField[sortFields.size()]));

		collector = TopFieldCollector.create(sort, hasMoreAmount, after, Integer.MAX_VALUE);
		return collector;
	}

	private ZuliaQuery.ScoredResult.Builder handleDocResult(IndexSearcher is, ZuliaQuery.SortRequest sortRequest, boolean sorting, ScoreDoc[] results, int i,
			ZuliaQuery.FetchType resultFetchType, List<String> fieldsToReturn, List<String> fieldsToMask, List<ZuliaHighlighter> highlighterList,
			List<AnalysisHandler> analysisHandlerList) throws Exception {
		int luceneShardId = results[i].doc;

		Set<String> fieldsToFetch = fetchSet;

		if (ZuliaQuery.FetchType.FULL.equals(resultFetchType)) {
			fieldsToFetch = fetchSetWithDocument;
		}
		else if (ZuliaQuery.FetchType.META.equals(resultFetchType)) {
			fieldsToFetch = fetchSetWithMeta;
		}

		Document d = is.doc(luceneShardId, fieldsToFetch);

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
			rdBuilder.setMetadata(ByteString.copyFrom(metaRef.bytes));
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
				continue;
			}

			ZuliaQuery.FieldSort fieldSort = sortRequest.getFieldSort(c);
			String sortField = fieldSort.getSortField();

			ZuliaIndex.FieldConfig.FieldType fieldTypeForSortField = indexConfig.getFieldTypeForSortField(sortField);

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
						TextFragment[] bestTextFragments = highlighter
								.getBestTextFragments(tokenStream, content, false, highlightRequest.getNumberOfFragments());
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

		Query query = new TermQuery(new org.apache.lucene.index.Term(ZuliaConstants.ID_FIELD, uniqueId));

		ZuliaQuery.ShardQueryResponse segmentResponse = this
				.queryShard(query, null, 1, null, null, null, null, resultFetchType, fieldsToReturn, fieldsToMask, Collections.emptyList(),
						Collections.emptyList(), false);

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

}
