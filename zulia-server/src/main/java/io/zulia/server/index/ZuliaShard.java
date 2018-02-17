package io.zulia.server.index;

import com.google.common.util.concurrent.UncheckedExecutionException;
import com.google.protobuf.ByteString;
import info.debatty.java.lsh.SuperBit;
import io.zulia.ZuliaConstants;
import io.zulia.message.ZuliaBase;
import io.zulia.message.ZuliaBase.FuzzyTerm;
import io.zulia.message.ZuliaBase.Metadata;
import io.zulia.message.ZuliaBase.ResultDocument;
import io.zulia.message.ZuliaBase.ShardCountResponse;
import io.zulia.message.ZuliaBase.Similarity;
import io.zulia.message.ZuliaIndex;
import io.zulia.message.ZuliaIndex.AnalyzerSettings;
import io.zulia.message.ZuliaIndex.FacetAs;
import io.zulia.message.ZuliaIndex.FieldConfig;
import io.zulia.message.ZuliaIndex.IndexAs;
import io.zulia.message.ZuliaIndex.SortAs;
import io.zulia.message.ZuliaQuery;
import io.zulia.message.ZuliaQuery.AnalysisResult;
import io.zulia.message.ZuliaQuery.CountRequest;
import io.zulia.message.ZuliaQuery.FacetCount;
import io.zulia.message.ZuliaQuery.FacetGroup;
import io.zulia.message.ZuliaQuery.FacetRequest;
import io.zulia.message.ZuliaQuery.FetchType;
import io.zulia.message.ZuliaQuery.FieldSort;
import io.zulia.message.ZuliaQuery.HighlightRequest;
import io.zulia.message.ZuliaQuery.HighlightResult;
import io.zulia.message.ZuliaQuery.ScoredResult;
import io.zulia.message.ZuliaQuery.ShardQueryResponse;
import io.zulia.message.ZuliaQuery.SortRequest;
import io.zulia.message.ZuliaQuery.SortValue;
import io.zulia.message.ZuliaQuery.SortValues;
import io.zulia.message.ZuliaServiceOuterClass.GetFieldNamesResponse;
import io.zulia.message.ZuliaServiceOuterClass.GetTermsRequest;
import io.zulia.message.ZuliaServiceOuterClass.GetTermsResponse;
import io.zulia.server.analysis.highlight.ZuliaHighlighter;
import io.zulia.server.analysis.similarity.ConstantSimilarity;
import io.zulia.server.analysis.similarity.TFSimilarity;
import io.zulia.server.config.ServerIndexConfig;
import io.zulia.server.index.field.BooleanFieldIndexer;
import io.zulia.server.index.field.DateFieldIndexer;
import io.zulia.server.index.field.DoubleFieldIndexer;
import io.zulia.server.index.field.FieldTypeUtil;
import io.zulia.server.index.field.FloatFieldIndexer;
import io.zulia.server.index.field.IntFieldIndexer;
import io.zulia.server.index.field.LongFieldIndexer;
import io.zulia.server.index.field.StringFieldIndexer;
import io.zulia.server.search.QueryCacheKey;
import io.zulia.server.search.QueryResultCache;
import io.zulia.util.ResultHelper;
import io.zulia.util.ZuliaUtil;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.analysis.miscellaneous.PerFieldAnalyzerWrapper;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.sortedset.DefaultSortedSetDocValuesReaderState;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetCounts;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.index.TermsEnum.SeekStatus;
import org.apache.lucene.search.*;
import org.apache.lucene.search.highlight.Fragmenter;
import org.apache.lucene.search.highlight.QueryScorer;
import org.apache.lucene.search.highlight.SimpleHTMLFormatter;
import org.apache.lucene.search.highlight.SimpleSpanFragmenter;
import org.apache.lucene.search.highlight.TextFragment;
import org.apache.lucene.search.similarities.BM25Similarity;
import org.apache.lucene.search.similarities.ClassicSimilarity;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

import java.io.IOException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ZuliaShard {

	private final static Logger log = Logger.getLogger(ZuliaShard.class.getSimpleName());
	private static Pattern sortedDocValuesMessage = Pattern.compile(
			"unexpected docvalues type NONE for field '(.*)' \\(expected one of \\[SORTED, SORTED_SET\\]\\)\\. Use UninvertingReader or index with docvalues\\.");
	private final int shardNumber;
	private final ServerIndexConfig indexConfig;
	private final AtomicLong counter;
	private final Set<String> fetchSet;
	private final Set<String> fetchSetWithMeta;
	private final Set<String> fetchSetWithDocument;
	private final IndexShardInterface indexShardInterface;

	private IndexWriter indexWriter;
	private DirectoryReader directoryReader;
	private DefaultSortedSetDocValuesReaderState sortedSetDocValuesReaderState;


	private Long lastCommit;
	private Long lastChange;
	private String indexName;
	private QueryResultCache queryResultCache;

	private FacetsConfig facetsConfig;
	private int segmentQueryCacheMaxAmount;
	private PerFieldAnalyzerWrapper perFieldAnalyzer;


	private final boolean primary;

	public ZuliaShard(int shardNumber, IndexShardInterface indexShardInterface, ServerIndexConfig indexConfig, FacetsConfig facetsConfig, boolean primary)
			throws Exception {
		setupCaches(indexConfig);

		this.primary = primary;

		this.shardNumber = shardNumber;

		this.indexShardInterface = indexShardInterface;
		this.indexConfig = indexConfig;

		openIndexWriters();

		this.facetsConfig = facetsConfig;

		this.fetchSet = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(ZuliaConstants.ID_FIELD, ZuliaConstants.TIMESTAMP_FIELD)));

		this.fetchSetWithMeta = Collections
				.unmodifiableSet(new HashSet<>(Arrays.asList(ZuliaConstants.ID_FIELD, ZuliaConstants.TIMESTAMP_FIELD, ZuliaConstants.STORED_META_FIELD)));

		this.fetchSetWithDocument = Collections.unmodifiableSet(new HashSet<>(
				Arrays.asList(ZuliaConstants.ID_FIELD, ZuliaConstants.TIMESTAMP_FIELD, ZuliaConstants.STORED_META_FIELD, ZuliaConstants.STORED_DOC_FIELD)));

		this.counter = new AtomicLong();
		this.lastCommit = null;
		this.lastChange = null;
		this.indexName = indexConfig.getIndexName();

	}

	public boolean isPrimary() {
		return primary;
	}

	private static String getFoldedString(String text) {

		boolean needsFolding = false;
		for (int pos = 0; pos < text.length(); ++pos) {
			final char c = text.charAt(pos);

			if (c >= '\u0080') {
				needsFolding = true;
				break;
			}
		}

		if (!needsFolding) {
			return text;
		}

		char[] textChar = text.toCharArray();
		char[] output = new char[textChar.length * 4];
		int outputPos = ASCIIFoldingFilter.foldToASCII(textChar, 0, output, 0, textChar.length);
		text = new String(output, 0, outputPos);
		return text;
	}

	public static void main(String[] args) {
		System.out.println(getFoldedString("Blah blah blah"));

		System.out.println(getFoldedString("Blah blḀh blḀh"));
	}

	private void reopenIndexWritersIfNecessary() throws Exception {

		synchronized (this) {
			if (!indexWriter.isOpen()) {

				if (!indexWriter.isOpen()) {
					this.indexWriter = this.indexShardInterface.getIndexWriter(shardNumber);
					this.directoryReader = DirectoryReader.open(indexWriter);
					this.sortedSetDocValuesReaderState = new DefaultSortedSetDocValuesReaderState(this.directoryReader);
				}
			}

		}

	}

	private void openIndexWriters() throws Exception {
		synchronized (this) {
			if (this.indexWriter != null) {
				indexWriter.close();
			}

			this.perFieldAnalyzer = this.indexShardInterface.getPerFieldAnalyzer();

			this.indexWriter = this.indexShardInterface.getIndexWriter(shardNumber);
			if (this.directoryReader != null) {
				this.directoryReader.close();
			}
			this.directoryReader = DirectoryReader.open(indexWriter);
			this.sortedSetDocValuesReaderState = new DefaultSortedSetDocValuesReaderState(this.directoryReader);

		}
	}

	private void setupCaches(ServerIndexConfig indexConfig) {
		segmentQueryCacheMaxAmount = indexConfig.getIndexSettings().getShardQueryCacheMaxAmount();

		int segmentQueryCacheSize = indexConfig.getIndexSettings().getShardQueryCacheSize();
		if ((segmentQueryCacheSize > 0)) {
			this.queryResultCache = new QueryResultCache(segmentQueryCacheSize, 8);
		}
		else {
			this.queryResultCache = null;
		}

	}

	public void updateIndexSettings() throws Exception {
		setupCaches(indexConfig);
		openIndexWriters();

	}

	public int getShardNumber() {
		return shardNumber;
	}

	public ShardQueryResponse queryShard(Query query, Map<String, Similarity> similarityOverrideMap, int amount, FieldDoc after, FacetRequest facetRequest,
			SortRequest sortRequest, QueryCacheKey queryCacheKey, FetchType resultFetchType, List<String> fieldsToReturn, List<String> fieldsToMask,
			List<HighlightRequest> highlightList, List<ZuliaQuery.AnalysisRequest> analysisRequestList, boolean debug) throws Exception {
		try {
			reopenIndexWritersIfNecessary();

			openReaderIfChanges();

			QueryResultCache qrc = queryResultCache;

			boolean useCache = (qrc != null) && ((segmentQueryCacheMaxAmount <= 0) || (segmentQueryCacheMaxAmount >= amount)) && queryCacheKey != null;
			if (useCache) {
				ShardQueryResponse cacheShardResponse = qrc.getCachedShardQueryResponse(queryCacheKey);
				if (cacheShardResponse != null) {
					return cacheShardResponse;
				}
			}

			IndexSearcher indexSearcher = new IndexSearcher(directoryReader);

			//similarity is only set query time, indexing time all these similarities are the same
			indexSearcher.setSimilarity(getSimilarity(similarityOverrideMap));

			if (debug) {
				log.info("Lucene Query for index <" + indexName + "> segment <" + shardNumber + ">: " + query);
				log.info("Rewritten Query for index <" + indexName + "> segment <" + shardNumber + ">: " + indexSearcher.rewrite(query));
			}

			int hasMoreAmount = amount + 1;

			TopDocsCollector<?> collector;

			boolean sorting = (sortRequest != null) && !sortRequest.getFieldSortList().isEmpty();
			if (sorting) {

				collector = getSortingCollector(sortRequest, hasMoreAmount, after);
			}
			else {
				collector = TopScoreDocCollector.create(hasMoreAmount, after);
			}

			ShardQueryResponse.Builder shardQueryReponseBuilder = ShardQueryResponse.newBuilder();

			if ((facetRequest != null) && !facetRequest.getCountRequestList().isEmpty()) {

				searchWithFacets(facetRequest, query, indexSearcher, collector, shardQueryReponseBuilder);

			}
			else {
				indexSearcher.search(query, collector);
			}

			ScoreDoc[] results = collector.topDocs().scoreDocs;

			int totalHits = collector.getTotalHits();

			shardQueryReponseBuilder.setTotalHits(totalHits);

			boolean moreAvailable = (results.length == hasMoreAmount);

			int numResults = Math.min(results.length, amount);

			List<ZuliaHighlighter> highlighterList = getHighlighterList(highlightList, query);

			List<AnalysisHandler> analysisHandlerList = getAnalysisHandlerList(analysisRequestList);

			for (int i = 0; i < numResults; i++) {
				ScoredResult.Builder srBuilder = handleDocResult(indexSearcher, sortRequest, sorting, results, i, resultFetchType, fieldsToReturn, fieldsToMask,
						highlighterList, analysisHandlerList);

				shardQueryReponseBuilder.addScoredResult(srBuilder.build());
			}

			if (moreAvailable) {
				ScoredResult.Builder srBuilder = handleDocResult(indexSearcher, sortRequest, sorting, results, numResults, FetchType.NONE,
						Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
				shardQueryReponseBuilder.setNext(srBuilder);
			}

			shardQueryReponseBuilder.setIndexName(indexName);
			shardQueryReponseBuilder.setShardNumber(shardNumber);

			if (!analysisHandlerList.isEmpty()) {
				for (AnalysisHandler analysisHandler : analysisHandlerList) {
					AnalysisResult segmentAnalysisResult = analysisHandler.getShardResult();
					if (segmentAnalysisResult != null) {
						shardQueryReponseBuilder.addAnalysisResult(segmentAnalysisResult);
					}
				}
			}

			ShardQueryResponse segmentResponse = shardQueryReponseBuilder.build();
			if (useCache) {
				qrc.storeInCache(queryCacheKey, segmentResponse);
			}
			return segmentResponse;
		}
		catch (IllegalStateException e) {
			Matcher m = sortedDocValuesMessage.matcher(e.getMessage());
			if (m.matches()) {
				String field = m.group(1);
				throw new Exception("Field <" + field + "> must have sortAs defined to be sortable");
			}

			throw e;
		}
	}

	private List<AnalysisHandler> getAnalysisHandlerList(List<ZuliaQuery.AnalysisRequest> analysisRequests) throws Exception {
		if (analysisRequests.isEmpty()) {
			return Collections.emptyList();
		}

		List<AnalysisHandler> analysisHandlerList = new ArrayList<>();
		for (ZuliaQuery.AnalysisRequest analysisRequest : analysisRequests) {

			Analyzer analyzer = perFieldAnalyzer;

			String analyzerOverride = analysisRequest.getAnalyzerOverride();
			if (analyzerOverride != null && !analyzerOverride.isEmpty()) {
				String analyzerName = analyzerOverride;

				AnalyzerSettings analyzerSettings = indexConfig.getAnalyzerSettingsByName(analyzerName);
				if (analyzerSettings != null) {
					analyzer = ZuliaAnalyzerFactory.getPerFieldAnalyzer(analyzerSettings);
				}
				else {
					throw new RuntimeException("Invalid analyzer name <" + analyzerName + ">");
				}
			}

			AnalysisHandler analysisHandler = new AnalysisHandler(directoryReader, analyzer, indexConfig, analysisRequest);
			analysisHandlerList.add(analysisHandler);
		}
		return analysisHandlerList;

	}

	private List<ZuliaHighlighter> getHighlighterList(List<HighlightRequest> highlightRequests, Query q) {

		if (highlightRequests.isEmpty()) {
			return Collections.emptyList();
		}

		List<ZuliaHighlighter> highlighterList = new ArrayList<>();

		for (HighlightRequest highlight : highlightRequests) {
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

	private PerFieldSimilarityWrapper getSimilarity(Map<String, Similarity> similarityOverrideMap) {
		return new PerFieldSimilarityWrapper() {
			@Override
			public org.apache.lucene.search.similarities.Similarity get(String name) {

				AnalyzerSettings analyzerSettings = indexConfig.getAnalyzerSettingsForIndexField(name);
				Similarity similarity = Similarity.BM25;
				if (analyzerSettings != null && analyzerSettings.getSimilarity() != null) {
					similarity = analyzerSettings.getSimilarity();
				}

				if (similarityOverrideMap != null) {
					Similarity fieldSimilarityOverride = similarityOverrideMap.get(name);
					if (fieldSimilarityOverride != null) {
						similarity = fieldSimilarityOverride;
					}
				}

				if (Similarity.TFIDF.equals(similarity)) {
					return new ClassicSimilarity();
				}
				else if (Similarity.BM25.equals(similarity)) {
					return new BM25Similarity();
				}
				else if (Similarity.CONSTANT.equals(similarity)) {
					return new ConstantSimilarity();
				}
				else if (Similarity.TF.equals(similarity)) {
					return new TFSimilarity();
				}
				else {
					throw new RuntimeException("Unknown similarity type <" + similarity + ">");
				}
			}
		};
	}

	private void searchWithFacets(FacetRequest facetRequest, Query q, IndexSearcher indexSearcher, TopDocsCollector<?> collector,
			ShardQueryResponse.Builder segmentReponseBuilder) throws Exception {
		FacetsCollector facetsCollector = new FacetsCollector();
		indexSearcher.search(q, MultiCollector.wrap(collector, facetsCollector));

		Facets facets = new SortedSetDocValuesFacetCounts(sortedSetDocValuesReaderState, facetsCollector);

		for (CountRequest countRequest : facetRequest.getCountRequestList()) {

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
						numOfFacets = sortedSetDocValuesReaderState.getSize();
					}
				}
				else {
					if (countRequest.getMaxFacets() > 0) {
						numOfFacets = countRequest.getMaxFacets();
					}
					else {
						numOfFacets = sortedSetDocValuesReaderState.getSize();
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
			FacetGroup.Builder fg = FacetGroup.newBuilder();
			fg.setCountRequest(countRequest);

			if (facetResult != null) {

				for (LabelAndValue subResult : facetResult.labelValues) {
					FacetCount.Builder facetCountBuilder = FacetCount.newBuilder();
					facetCountBuilder.setCount(subResult.value.longValue());
					facetCountBuilder.setFacet(subResult.label);
					fg.addFacetCount(facetCountBuilder);
				}
			}
			segmentReponseBuilder.addFacetGroup(fg);
		}
	}

	private TopDocsCollector<?> getSortingCollector(SortRequest sortRequest, int hasMoreAmount, FieldDoc after) throws Exception {
		List<SortField> sortFields = new ArrayList<>();
		TopDocsCollector<?> collector;
		for (FieldSort fs : sortRequest.getFieldSortList()) {
			boolean reverse = FieldSort.Direction.DESCENDING.equals(fs.getDirection());

			String sortField = fs.getSortField();
			FieldConfig.FieldType sortFieldType = indexConfig.getFieldTypeForSortField(sortField);

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

		collector = TopFieldCollector.create(sort, hasMoreAmount, after, true, true, true);
		return collector;
	}

	private void openReaderIfChanges() throws IOException {
		synchronized (this) {
			DirectoryReader newDirectoryReader = DirectoryReader.openIfChanged(directoryReader, indexWriter);
			if (newDirectoryReader != null) {
				directoryReader = newDirectoryReader;
				QueryResultCache qrc = queryResultCache;
				if (qrc != null) {
					qrc.clear();
				}

				sortedSetDocValuesReaderState = new DefaultSortedSetDocValuesReaderState(directoryReader);

			}

		}
	}

	private ScoredResult.Builder handleDocResult(IndexSearcher is, SortRequest sortRequest, boolean sorting, ScoreDoc[] results, int i,
			FetchType resultFetchType, List<String> fieldsToReturn, List<String> fieldsToMask, List<ZuliaHighlighter> highlighterList,
			List<AnalysisHandler> analysisHandlerList) throws Exception {
		int luceneShardId = results[i].doc;

		Set<String> fieldsToFetch = fetchSet;

		if (FetchType.FULL.equals(resultFetchType)) {
			fieldsToFetch = fetchSetWithDocument;
		}
		else if (FetchType.META.equals(resultFetchType)) {
			fieldsToFetch = fetchSetWithMeta;
		}

		Document d = is.doc(luceneShardId, fieldsToFetch);

		IndexableField f = d.getField(ZuliaConstants.TIMESTAMP_FIELD);
		long timestamp = f.numericValue().longValue();

		ScoredResult.Builder srBuilder = ScoredResult.newBuilder();
		String uniqueId = d.get(ZuliaConstants.ID_FIELD);

		if (!highlighterList.isEmpty() && !FetchType.FULL.equals(resultFetchType)) {
			throw new Exception("Highlighting requires a full fetch of the document");
		}

		if (!analysisHandlerList.isEmpty() && !FetchType.FULL.equals(resultFetchType)) {
			throw new Exception("Analysis requires a full fetch of the document");
		}

		if (!FetchType.NONE.equals(resultFetchType)) {
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

	private void handleStoredDoc(ScoredResult.Builder srBuilder, String uniqueId, Document d, FetchType resultFetchType, List<String> fieldsToReturn,
			List<String> fieldsToMask, List<ZuliaHighlighter> highlighterList, List<AnalysisHandler> analysisHandlerList) throws Exception {

		ResultDocument.Builder rdBuilder = ResultDocument.newBuilder();
		rdBuilder.setUniqueId(uniqueId);
		rdBuilder.setIndexName(indexName);

		if (FetchType.FULL.equals(resultFetchType) || FetchType.META.equals(resultFetchType)) {
			BytesRef metaRef = d.getBinaryValue(ZuliaConstants.STORED_META_FIELD);
			org.bson.Document metaMongoDoc = new org.bson.Document();
			metaMongoDoc.putAll(ZuliaUtil.byteArrayToMongoDocument(metaRef.bytes));

			for (String key : metaMongoDoc.keySet()) {
				rdBuilder.addMetadata(Metadata.newBuilder().setKey(key).setValue(((String) metaMongoDoc.get(key))));
			}
		}

		if (FetchType.FULL.equals(resultFetchType)) {
			BytesRef docRef = d.getBinaryValue(ZuliaConstants.STORED_DOC_FIELD);
			if (docRef != null) {
				rdBuilder.setDocument(ByteString.copyFrom(docRef.bytes));
			}
		}

		ResultDocument resultDocument = rdBuilder.build();

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

	private void handleSortValues(SortRequest sortRequest, ScoreDoc scoreDoc, ScoredResult.Builder srBuilder) {
		FieldDoc result = (FieldDoc) scoreDoc;

		SortValues.Builder sortValues = SortValues.newBuilder();

		int c = 0;
		for (Object o : result.fields) {
			if (o == null) {
				sortValues.addSortValue(SortValue.newBuilder().setExists(false));
				continue;
			}

			FieldSort fieldSort = sortRequest.getFieldSort(c);
			String sortField = fieldSort.getSortField();

			FieldConfig.FieldType fieldTypeForSortField = indexConfig.getFieldTypeForSortField(sortField);

			SortValue.Builder sortValueBuilder = SortValue.newBuilder().setExists(true);
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

	private void handleHighlight(List<ZuliaHighlighter> highlighterList, ScoredResult.Builder srBuilder, org.bson.Document doc) {

		for (ZuliaHighlighter highlighter : highlighterList) {
			HighlightRequest highlightRequest = highlighter.getHighlight();
			String indexField = highlightRequest.getField();
			String storedFieldName = indexConfig.getStoredFieldName(indexField);

			if (storedFieldName != null) {
				HighlightResult.Builder highLightResult = HighlightResult.newBuilder();
				highLightResult.setField(storedFieldName);

				Object storeFieldValues = ResultHelper.getValueFromMongoDocument(doc, storedFieldName);

				ZuliaUtil.handleLists(storeFieldValues, (value) -> {
					String content = value.toString();
					TokenStream tokenStream = perFieldAnalyzer.tokenStream(indexField, content);

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

	public ResultDocument getSourceDocument(String uniqueId, FetchType resultFetchType, List<String> fieldsToReturn, List<String> fieldsToMask)
			throws Exception {

		ResultDocument rd = null;

		Query query = new TermQuery(new org.apache.lucene.index.Term(ZuliaConstants.ID_FIELD, uniqueId));

		ShardQueryResponse segmentResponse = this
				.queryShard(query, null, 1, null, null, null, null, resultFetchType, fieldsToReturn, fieldsToMask, Collections.emptyList(),
						Collections.emptyList(), false);

		List<ScoredResult> scoredResultList = segmentResponse.getScoredResultList();
		if (!scoredResultList.isEmpty()) {
			ScoredResult scoredResult = scoredResultList.iterator().next();
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

		ResultDocument.Builder rdBuilder = ResultDocument.newBuilder();
		rdBuilder.setUniqueId(uniqueId);
		rdBuilder.setIndexName(indexName);
		return rdBuilder.build();

	}

	private ResultDocument filterDocument(ResultDocument rd, List<String> fieldsToReturn, List<String> fieldsToMask, org.bson.Document mongoDocument) {

		ResultDocument.Builder resultDocBuilder = rd.toBuilder();

		if (!fieldsToReturn.isEmpty()) {
			for (String key : new ArrayList<>(mongoDocument.keySet())) {
				if (!fieldsToReturn.contains(key)) {
					mongoDocument.remove(key);
				}
			}
		}
		if (!fieldsToMask.isEmpty()) {
			for (String field : fieldsToMask) {
				mongoDocument.remove(field);
			}
		}

		ByteString document = ByteString.copyFrom(ZuliaUtil.mongoDocumentToByteArray(mongoDocument));
		resultDocBuilder.setDocument(document);

		return resultDocBuilder.build();

	}

	private void possibleCommit() throws IOException {
		lastChange = System.currentTimeMillis();

		long count = counter.incrementAndGet();
		if ((count % indexConfig.getIndexSettings().getShardCommitInterval()) == 0) {
			forceCommit();
		}

	}

	public void forceCommit() throws IOException {
		if (!primary) {
			throw new IllegalStateException("Cannot force commit from replica:  index <" + indexName + "> shard <" + shardNumber + ">");
		}

		log.info("Committing shard <" + shardNumber + "> for index <" + indexName + ">");
		long currentTime = System.currentTimeMillis();
		indexWriter.commit();

		lastCommit = currentTime;

	}

	public void doCommit() throws IOException {

		long currentTime = System.currentTimeMillis();

		Long lastCh = lastChange;
		// if changes since started

		if (lastCh != null) {
			if ((currentTime - lastCh) > (indexConfig.getIndexSettings().getIdleTimeWithoutCommit() * 1000)) {
				if ((lastCommit == null) || (lastCh > lastCommit)) {
					forceCommit();
				}
			}
		}
	}

	public void close(boolean terminate) throws IOException {
		if (!terminate) {
			forceCommit();
		}

		Directory directory = indexWriter.getDirectory();
		indexWriter.close();
		directory.close();

	}

	public void index(String uniqueId, long timestamp, org.bson.Document mongoDocument, List<Metadata> metadataList) throws Exception {
		if (!primary) {
			throw new IllegalStateException("Cannot index document <" + uniqueId + "> from replica:  index <" + indexName + "> shard <" + shardNumber + ">");
		}

		reopenIndexWritersIfNecessary();

		Document luceneDocument = new Document();

		addStoredFieldsForDocument(mongoDocument, luceneDocument);

		luceneDocument.add(new StringField(ZuliaConstants.ID_FIELD, uniqueId, Store.YES));

		luceneDocument.add(new LongPoint(ZuliaConstants.TIMESTAMP_FIELD, timestamp));
		luceneDocument.add(new StoredField(ZuliaConstants.TIMESTAMP_FIELD, timestamp));

		luceneDocument.add(new StoredField(ZuliaConstants.STORED_DOC_FIELD, new BytesRef(ZuliaUtil.mongoDocumentToByteArray(mongoDocument))));

		org.bson.Document metadataMongoDoc = new org.bson.Document();

		for (Metadata metadata : metadataList) {
			metadataMongoDoc.put(metadata.getKey(), metadata.getValue());
		}

		luceneDocument.add(new StoredField(ZuliaConstants.STORED_META_FIELD, new BytesRef(ZuliaUtil.mongoDocumentToByteArray(metadataMongoDoc))));

		luceneDocument = facetsConfig.build(luceneDocument);

		Term term = new Term(ZuliaConstants.ID_FIELD, uniqueId);

		indexWriter.updateDocument(term, luceneDocument);

		possibleCommit();
	}

	private void addStoredFieldsForDocument(org.bson.Document mongoDocument, Document luceneDocument) throws Exception {
		for (String storedFieldName : indexConfig.getIndexedStoredFieldNames()) {

			FieldConfig fc = indexConfig.getFieldConfig(storedFieldName);

			if (fc != null) {

				FieldConfig.FieldType fieldType = fc.getFieldType();

				Object o = ResultHelper.getValueFromMongoDocument(mongoDocument, storedFieldName);

				if (o != null) {
					handleFacetsForStoredField(luceneDocument, fc, o);

					handleSortForStoredField(luceneDocument, storedFieldName, fc, o);

					handleIndexingForStoredField(luceneDocument, storedFieldName, fc, fieldType, o);

					handleProjectForStoredField(luceneDocument, fc, o);
				}
			}

		}
	}

	private void handleProjectForStoredField(Document luceneDocument, FieldConfig fc, Object o) throws Exception {
		for (ZuliaIndex.ProjectAs projectAs : fc.getProjectAsList()) {
			if (projectAs.hasSuperbit()) {
				if (o instanceof List) {
					List<Number> values = (List<Number>) o;

					double vec[] = new double[values.size()];
					int i = 0;
					for (Number value : values) {
						vec[i++] = value.doubleValue();
					}

					SuperBit superBitForField = indexConfig.getSuperBitForField(projectAs.getField());
					boolean[] signature = superBitForField.signature(vec);

					int j = 0;
					for (boolean s : signature) {
						StringFieldIndexer.INSTANCE.index(luceneDocument, projectAs.getField(), s ? "1" : "0",
								ZuliaConstants.SUPERBIT_PREFIX + "." + projectAs.getField() + "." + j);
						j++;
					}

				}
				else {
					throw new Exception("Expecting a list for superbit field <" + projectAs.getField() + ">");
				}
			}
		}
	}

	private void handleIndexingForStoredField(Document luceneDocument, String storedFieldName, FieldConfig fc, FieldConfig.FieldType fieldType, Object o)
			throws Exception {
		for (IndexAs indexAs : fc.getIndexAsList()) {

			String indexedFieldName = indexAs.getIndexFieldName();
			luceneDocument.add(new StringField(ZuliaConstants.FIELDS_LIST_FIELD, indexedFieldName, Store.NO));

			if (FieldConfig.FieldType.NUMERIC_INT.equals(fieldType)) {
				IntFieldIndexer.INSTANCE.index(luceneDocument, storedFieldName, o, indexedFieldName);
			}
			else if (FieldConfig.FieldType.NUMERIC_LONG.equals(fieldType)) {
				LongFieldIndexer.INSTANCE.index(luceneDocument, storedFieldName, o, indexedFieldName);
			}
			else if (FieldConfig.FieldType.NUMERIC_FLOAT.equals(fieldType)) {
				FloatFieldIndexer.INSTANCE.index(luceneDocument, storedFieldName, o, indexedFieldName);
			}
			else if (FieldConfig.FieldType.NUMERIC_DOUBLE.equals(fieldType)) {
				DoubleFieldIndexer.INSTANCE.index(luceneDocument, storedFieldName, o, indexedFieldName);
			}
			else if (FieldConfig.FieldType.DATE.equals(fieldType)) {
				DateFieldIndexer.INSTANCE.index(luceneDocument, storedFieldName, o, indexedFieldName);
			}
			else if (FieldConfig.FieldType.BOOL.equals(fieldType)) {
				BooleanFieldIndexer.INSTANCE.index(luceneDocument, storedFieldName, o, indexedFieldName);
			}
			else if (FieldConfig.FieldType.STRING.equals(fieldType)) {
				StringFieldIndexer.INSTANCE.index(luceneDocument, storedFieldName, o, indexedFieldName);
			}
			else {
				throw new RuntimeException("Unsupported field type <" + fieldType + ">");
			}
		}
	}

	private void handleSortForStoredField(Document d, String storedFieldName, FieldConfig fc, Object o) {

		FieldConfig.FieldType fieldType = fc.getFieldType();
		for (SortAs sortAs : fc.getSortAsList()) {
			String sortFieldName = sortAs.getSortFieldName();

			if (FieldTypeUtil.isNumericOrDateFieldType(fieldType)) {
				ZuliaUtil.handleLists(o, obj -> {

					if (FieldConfig.FieldType.DATE.equals(fieldType)) {
						if (obj instanceof Date) {

							Date date = (Date) obj;
							SortedNumericDocValuesField docValue = new SortedNumericDocValuesField(sortFieldName, date.getTime());
							d.add(docValue);
						}
						else {
							throw new RuntimeException(
									"Expecting date for document field <" + storedFieldName + "> / sort field <" + sortFieldName + ">, found <" + o.getClass()
											+ ">");
						}
					}
					else {
						if (obj instanceof Number) {

							Number number = (Number) obj;
							SortedNumericDocValuesField docValue = null;
							if (FieldConfig.FieldType.NUMERIC_INT.equals(fieldType)) {
								docValue = new SortedNumericDocValuesField(sortFieldName, number.intValue());
							}
							else if (FieldConfig.FieldType.NUMERIC_LONG.equals(fieldType)) {
								docValue = new SortedNumericDocValuesField(sortFieldName, number.longValue());
							}
							else if (FieldConfig.FieldType.NUMERIC_FLOAT.equals(fieldType)) {
								docValue = new SortedNumericDocValuesField(sortFieldName, NumericUtils.floatToSortableInt(number.floatValue()));
							}
							else if (FieldConfig.FieldType.NUMERIC_DOUBLE.equals(fieldType)) {
								docValue = new SortedNumericDocValuesField(sortFieldName, NumericUtils.doubleToSortableLong(number.doubleValue()));
							}
							else {
								throw new RuntimeException(
										"Not handled numeric field type <" + fieldType + "> for document field <" + storedFieldName + "> / sort field <"
												+ sortFieldName + ">");
							}

							d.add(docValue);
						}
						else {
							throw new RuntimeException(
									"Expecting number for document field <" + storedFieldName + "> / sort field <" + sortFieldName + ">, found <" + o.getClass()
											+ ">");
						}
					}
				});
			}
			else if (FieldConfig.FieldType.BOOL.equals(fieldType)) {
				ZuliaUtil.handleLists(o, obj -> {
					if (obj instanceof Boolean) {
						String text = obj.toString();
						SortedSetDocValuesField docValue = new SortedSetDocValuesField(sortFieldName, new BytesRef(text));
						d.add(docValue);
					}
					else {
						throw new RuntimeException(
								"Expecting boolean for document field <" + storedFieldName + "> / sort field <" + sortFieldName + ">, found <" + o.getClass()
										+ ">");
					}
				});
			}
			else if (FieldConfig.FieldType.STRING.equals(fieldType)) {
				ZuliaUtil.handleLists(o, obj -> {
					String text = o.toString();

					SortAs.StringHandling stringHandling = sortAs.getStringHandling();
					if (SortAs.StringHandling.STANDARD.equals(stringHandling)) {
						//no op
					}
					else if (SortAs.StringHandling.LOWERCASE.equals(stringHandling)) {
						text = text.toLowerCase();
					}
					else if (SortAs.StringHandling.FOLDING.equals(stringHandling)) {
						text = getFoldedString(text);
					}
					else if (SortAs.StringHandling.LOWERCASE_FOLDING.equals(stringHandling)) {
						text = getFoldedString(text).toLowerCase();
					}
					else {
						throw new RuntimeException(
								"Not handled string handling <" + stringHandling + "> for document field <" + storedFieldName + "> / sort field <"
										+ sortFieldName + ">");
					}

					SortedSetDocValuesField docValue = new SortedSetDocValuesField(sortFieldName, new BytesRef(text));
					d.add(docValue);
				});
			}
			else {
				throw new RuntimeException(
						"Not handled field type <" + fieldType + "> for document field <" + storedFieldName + "> / sort field <" + sortFieldName + ">");
			}

		}
	}

	private void handleFacetsForStoredField(Document doc, FieldConfig fc, Object o) throws Exception {
		for (FacetAs fa : fc.getFacetAsList()) {

			String facetName = fa.getFacetName();

			if (FieldConfig.FieldType.DATE.equals(fc.getFieldType())) {
				FacetAs.DateHandling dateHandling = fa.getDateHandling();
				ZuliaUtil.handleLists(o, obj -> {
					if (obj instanceof Date) {
						LocalDate localDate = ((Date) (obj)).toInstant().atZone(ZoneId.of("UTC")).toLocalDate();

						if (FacetAs.DateHandling.DATE_YYYYMMDD.equals(dateHandling)) {
							String date = localDate.getYear() + "" + localDate.getMonthValue() + "" + localDate.getDayOfMonth();
							addFacet(doc, facetName, date);
						}
						else if (FacetAs.DateHandling.DATE_YYYY_MM_DD.equals(dateHandling)) {

							String date = localDate.getYear() + "-" + localDate.getMonthValue() + "-" + localDate.getDayOfMonth();
							addFacet(doc, facetName, date);
						}
						else {
							throw new RuntimeException("Not handled date handling <" + dateHandling + "> for facet <" + fa.getFacetName() + ">");
						}

					}
					else {
						throw new RuntimeException("Cannot facet date for document field <" + fc.getStoredFieldName() + "> / facet <" + fa.getFacetName()
								+ ">: excepted Date or Collection of Date, found <" + o.getClass().getSimpleName() + ">");
					}
				});
			}
			else {
				ZuliaUtil.handleLists(o, obj -> {
					String string = obj.toString();
					addFacet(doc, facetName, string);
				});
			}

		}
	}

	private void addFacet(Document doc, String facetName, String value) {
		if (!value.isEmpty()) {
			doc.add(new SortedSetDocValuesFacetField(facetName, value));
			doc.add(new StringField(FacetsConfig.DEFAULT_INDEX_FIELD_NAME + "." + facetName, new BytesRef(value), Store.NO));
		}
	}

	public void deleteDocument(String uniqueId) throws Exception {
		if (!primary) {
			throw new IllegalStateException("Cannot delete document <" + uniqueId + "> from replica:  index <" + indexName + "> shard <" + shardNumber + ">");
		}

		Term term = new Term(ZuliaConstants.ID_FIELD, uniqueId);
		indexWriter.deleteDocuments(term);
		possibleCommit();

	}

	public void optimize(int maxNumberSegments) throws IOException {
		if (!primary) {
			throw new IllegalStateException("Cannot optimize replica:  index <" + indexName + "> shard <" + shardNumber + ">");
		}

		lastChange = System.currentTimeMillis();
		indexWriter.forceMerge(maxNumberSegments);
		forceCommit();
	}

	public GetFieldNamesResponse getFieldNames() throws IOException {

		openReaderIfChanges();

		GetFieldNamesResponse.Builder builder = GetFieldNamesResponse.newBuilder();

		Set<String> fields = new HashSet<>();

		for (LeafReaderContext subReaderContext : directoryReader.leaves()) {
			FieldInfos fieldInfos = subReaderContext.reader().getFieldInfos();
			for (FieldInfo fi : fieldInfos) {
				String fieldName = fi.name;
				fields.add(fieldName);
			}
		}

		fields.forEach(builder::addFieldName);

		return builder.build();
	}

	public void clear() throws IOException {
		if (!primary) {
			throw new IllegalStateException("Cannot clear replica:  index <" + indexName + "> shard <" + shardNumber + ">");
		}
		// index has write lock so none needed here
		indexWriter.deleteAll();
		forceCommit();
	}

	public GetTermsResponse getTerms(GetTermsRequest request) throws IOException {
		openReaderIfChanges();

		GetTermsResponse.Builder builder = GetTermsResponse.newBuilder();

		String fieldName = request.getFieldName();

		SortedMap<String, ZuliaBase.Term.Builder> termsMap = new TreeMap<>();

		if (request.getIncludeTermCount() > 0) {

			Set<String> includeTerms = new TreeSet<>(request.getIncludeTermList());
			List<BytesRef> termBytesList = new ArrayList<>();
			for (String term : includeTerms) {
				BytesRef termBytes = new BytesRef(term);
				termBytesList.add(termBytes);
			}

			for (LeafReaderContext subReaderContext : directoryReader.leaves()) {
				Terms terms = subReaderContext.reader().terms(fieldName);

				if (terms != null) {

					TermsEnum termsEnum = terms.iterator();
					for (BytesRef termBytes : termBytesList) {
						if (termsEnum.seekExact(termBytes)) {
							BytesRef text = termsEnum.term();
							handleTerm(termsMap, termsEnum, text, null, null);
						}

					}
				}

			}
		}
		else {

			AttributeSource atts = null;
			MaxNonCompetitiveBoostAttribute maxBoostAtt = null;
			boolean hasFuzzyTerm = request.hasFuzzyTerm();
			if (hasFuzzyTerm) {
				atts = new AttributeSource();
				maxBoostAtt = atts.addAttribute(MaxNonCompetitiveBoostAttribute.class);
			}

			BytesRef startTermBytes;
			BytesRef endTermBytes = null;

			if (!request.getStartTerm().isEmpty()) {
				startTermBytes = new BytesRef(request.getStartTerm());
			}
			else {
				startTermBytes = new BytesRef("");
			}

			if (!request.getEndTerm().isEmpty()) {
				endTermBytes = new BytesRef(request.getEndTerm());
			}

			Pattern termFilter = null;
			if (!request.getTermFilter().isEmpty()) {
				termFilter = Pattern.compile(request.getTermFilter());
			}

			Pattern termMatch = null;
			if (!request.getTermMatch().isEmpty()) {
				termMatch = Pattern.compile(request.getTermMatch());
			}

			for (LeafReaderContext subReaderContext : directoryReader.leaves()) {
				Terms terms = subReaderContext.reader().terms(fieldName);

				if (terms != null) {

					if (hasFuzzyTerm) {
						FuzzyTerm fuzzyTerm = request.getFuzzyTerm();
						FuzzyTermsEnum termsEnum = new FuzzyTermsEnum(terms, atts, new Term(fieldName, fuzzyTerm.getTerm()), fuzzyTerm.getEditDistance(),
								fuzzyTerm.getPrefixLength(), !fuzzyTerm.getNoTranspositions());
						BytesRef text = termsEnum.term();

						handleTerm(termsMap, termsEnum, text, termFilter, termMatch);

						while ((text = termsEnum.next()) != null) {
							handleTerm(termsMap, termsEnum, text, termFilter, termMatch);
						}

					}
					else {
						TermsEnum termsEnum = terms.iterator();
						SeekStatus seekStatus = termsEnum.seekCeil(startTermBytes);

						if (!seekStatus.equals(SeekStatus.END)) {
							BytesRef text = termsEnum.term();

							if (endTermBytes == null || (text.compareTo(endTermBytes) < 0)) {
								handleTerm(termsMap, termsEnum, text, termFilter, termMatch);

								while ((text = termsEnum.next()) != null) {

									if (endTermBytes == null || (text.compareTo(endTermBytes) < 0)) {
										handleTerm(termsMap, termsEnum, text, termFilter, termMatch);
									}
									else {
										break;
									}
								}
							}
						}
					}

				}

			}
		}

		for (ZuliaBase.Term.Builder termBuilder : termsMap.values()) {
			builder.addTerm(termBuilder.build());
		}

		return builder.build();

	}

	private void handleTerm(SortedMap<String, ZuliaBase.Term.Builder> termsMap, TermsEnum termsEnum, BytesRef text, Pattern termFilter, Pattern termMatch)
			throws IOException {

		String textStr = text.utf8ToString();
		if (termFilter != null || termMatch != null) {

			if (termFilter != null) {
				if (termFilter.matcher(textStr).matches()) {
					return;
				}
			}

			if (termMatch != null) {
				if (!termMatch.matcher(textStr).matches()) {
					return;
				}
			}
		}

		if (!termsMap.containsKey(textStr)) {
			termsMap.put(textStr, ZuliaBase.Term.newBuilder().setValue(textStr).setDocFreq(0).setTermFreq(0));
		}
		ZuliaBase.Term.Builder builder = termsMap.get(textStr);
		builder.setDocFreq(builder.getDocFreq() + termsEnum.docFreq());
		builder.setTermFreq(builder.getTermFreq() + termsEnum.totalTermFreq());
		BoostAttribute boostAttribute = termsEnum.attributes().getAttribute(BoostAttribute.class);
		if (boostAttribute != null) {
			builder.setScore(boostAttribute.getBoost());
		}

	}

	public ShardCountResponse getNumberOfDocs() throws IOException {

		openReaderIfChanges();
		int count = directoryReader.numDocs();
		return ShardCountResponse.newBuilder().setNumberOfDocs(count).setShardNumber(shardNumber).build();

	}

}
