package io.zulia.server.search;

import io.zulia.message.ZuliaBase.Term;
import io.zulia.message.ZuliaIndex.FieldConfig;
import io.zulia.message.ZuliaQuery;
import io.zulia.message.ZuliaQuery.AnalysisRequest;
import io.zulia.message.ZuliaQuery.AnalysisResult;
import io.zulia.message.ZuliaQuery.CountRequest;
import io.zulia.message.ZuliaQuery.FacetGroup;
import io.zulia.message.ZuliaQuery.FieldSort;
import io.zulia.message.ZuliaQuery.IndexShardResponse;
import io.zulia.message.ZuliaQuery.LastIndexResult;
import io.zulia.message.ZuliaQuery.LastResult;
import io.zulia.message.ZuliaQuery.ScoredResult;
import io.zulia.message.ZuliaQuery.ShardQueryResponse;
import io.zulia.message.ZuliaQuery.SortRequest;
import io.zulia.message.ZuliaQuery.StatRequest;
import io.zulia.message.ZuliaServiceOuterClass.InternalQueryResponse;
import io.zulia.message.ZuliaServiceOuterClass.QueryRequest;
import io.zulia.message.ZuliaServiceOuterClass.QueryResponse;
import io.zulia.server.analysis.frequency.TermFreq;
import io.zulia.server.config.SortFieldInfo;
import io.zulia.server.index.ZuliaIndex;
import io.zulia.server.search.aggregation.facets.FacetCombiner;
import io.zulia.server.search.aggregation.stats.StatCombiner;
import io.zulia.server.search.score.ZuliaPostSortingComparator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QueryCombiner {

	private final static Logger LOG = LoggerFactory.getLogger(QueryCombiner.class);
	private final List<InternalQueryResponse> responses;
	private final Map<String, Map<Integer, ShardQueryResponse>> indexToShardQueryResponseMap;
	private final List<ShardQueryResponse> shardResponses;
	private final int amount;
	private final int start;
	private final LastResult lastResult;
	private final List<AnalysisRequest> analysisRequestList;
	private final SortRequest sortRequest;
	private final Collection<ZuliaIndex> indexes;
	private final Map<String, Integer> indexToShardCount;
	private boolean isShort;

	public QueryCombiner(Collection<ZuliaIndex> indexes, QueryRequest request, List<InternalQueryResponse> responses) {
		this.indexToShardCount = new HashMap<>();

		for (ZuliaIndex zuliaIndex : indexes) {
			indexToShardCount.put(zuliaIndex.getIndexName(), zuliaIndex.getNumberOfShards());
		}

		this.indexes = indexes;

		this.responses = responses;
		this.amount = request.getAmount() + request.getStart();
		this.indexToShardQueryResponseMap = new HashMap<>();
		this.shardResponses = new ArrayList<>();
		this.lastResult = request.getLastResult();
		this.sortRequest = request.getSortRequest();
		this.start = request.getStart();
		this.analysisRequestList = request.getAnalysisRequestList();

		this.isShort = false;

	}

	private void validate() throws Exception {
		for (InternalQueryResponse iqr : responses) {

			for (IndexShardResponse isr : iqr.getIndexShardResponseList()) {
				String indexName = isr.getIndexName();
				if (!indexToShardQueryResponseMap.containsKey(indexName)) {
					indexToShardQueryResponseMap.put(indexName, new HashMap<>());
				}

				for (ShardQueryResponse sr : isr.getShardQueryResponseList()) {
					int shardNumber = sr.getShardNumber();

					Map<Integer, ShardQueryResponse> shardResponseMap = indexToShardQueryResponseMap.get(indexName);

					if (shardResponseMap.containsKey(shardNumber)) {
						throw new Exception("Shard " + shardNumber + " is repeated for " + indexName);
					}
					else {
						shardResponseMap.put(shardNumber, sr);
						shardResponses.add(sr);
					}
				}

			}

		}

		for (ZuliaIndex index : indexes) {
			int numberOfShards = index.getNumberOfShards();
			Map<Integer, ShardQueryResponse> shardResponseMap = indexToShardQueryResponseMap.get(index.getIndexName());

			if (shardResponseMap == null) {
				throw new Exception("Missing index " + index.getIndexName() + "> in response");
			}

			if (shardResponseMap.size() != numberOfShards) {
				throw new Exception("Found " + shardResponseMap.size() + " expected " + numberOfShards);
			}

			for (int shardNumber = 0; shardNumber < numberOfShards; shardNumber++) {
				if (!shardResponseMap.containsKey(shardNumber)) {
					throw new Exception("Missing shard " + shardNumber);
				}
			}
		}
	}

	public QueryResponse getQueryResponse() throws Exception {

		validate();

		long totalHits = 0;
		long returnedHits = 0;
		int shardsCached = 0;
		int shardsPinned = 0;

		for (ShardQueryResponse sr : shardResponses) {
			totalHits += sr.getTotalHits();
			returnedHits += sr.getScoredResultList().size();
			if (sr.getCached()) {
				shardsCached++;
			}
			if (sr.getPinned()) {
				shardsPinned++;
			}

		}

		boolean fullyCached = shardsCached == shardResponses.size();

		QueryResponse.Builder builder = QueryResponse.newBuilder();
		builder.setTotalHits(totalHits);
		builder.setFullyCached(fullyCached);
		builder.setShardsCached(shardsCached);
		builder.setShardsPinned(shardsPinned);
		builder.setShardsQueried(shardResponses.size());

		int resultsSize = Math.min(amount, (int) returnedHits);

		Map<CountRequest, FacetCombiner> facetCombinerMap = new HashMap<>();
		Map<StatRequest, StatCombiner> statCombinerMap = new HashMap<>();

		Map<AnalysisRequest, Map<String, Term.Builder>> analysisRequestToTermMap = new HashMap<>();

		int shardIndex = 0;

		for (ShardQueryResponse sr : shardResponses) {
			for (FacetGroup fg : sr.getFacetGroupList()) {
				CountRequest countRequest = fg.getCountRequest();
				FacetCombiner facetCombiner = facetCombinerMap.computeIfAbsent(countRequest,
						countRequest1 -> new FacetCombiner(countRequest, shardResponses.size()));
				facetCombiner.handleFacetGroupForShard(fg, shardIndex);
			}

			for (ZuliaQuery.StatGroupInternal sg : sr.getStatGroupList()) {
				StatRequest statRequest = sg.getStatRequest();
				StatCombiner statCombiner = statCombinerMap.computeIfAbsent(statRequest, statRequest1 -> new StatCombiner(statRequest, shardResponses.size()));
				statCombiner.handleStatGroupForShard(sg, shardIndex);
			}

			for (AnalysisResult analysisResult : sr.getAnalysisResultList()) {

				AnalysisRequest analysisRequest = analysisResult.getAnalysisRequest();
				if (!analysisRequestToTermMap.containsKey(analysisRequest)) {
					analysisRequestToTermMap.put(analysisRequest, new HashMap<>());
				}

				Map<String, Term.Builder> termMap = analysisRequestToTermMap.get(analysisRequest);

				for (Term term : analysisResult.getTermsList()) {
					String key = term.getValue();
					if (!termMap.containsKey(key)) {
						termMap.put(key, Term.newBuilder().setValue(key).setDocFreq(0).setTermFreq(0));
					}
					Term.Builder termsBuilder = termMap.get(key);
					termsBuilder.setDocFreq(termsBuilder.getDocFreq() + term.getDocFreq());
					termsBuilder.setScore(termsBuilder.getScore() + term.getScore());
					termsBuilder.setTermFreq(termsBuilder.getTermFreq() + term.getTermFreq());
				}
			}

			shardIndex++;
		}

		for (AnalysisRequest analysisRequest : analysisRequestList) {
			Map<String, Term.Builder> termMap = analysisRequestToTermMap.get(analysisRequest);
			if (termMap != null) {
				List<Term.Builder> terms = new ArrayList<>(termMap.values());
				List<Term.Builder> topTerms = TermFreq.getTopTerms(terms, analysisRequest.getTopN(), analysisRequest.getTermSort());
				AnalysisResult.Builder analysisResultBuilder = AnalysisResult.newBuilder().setAnalysisRequest(analysisRequest);
				topTerms.forEach(analysisResultBuilder::addTerms);
				builder.addAnalysisResult(analysisResultBuilder);
			}
		}

		for (FacetCombiner facetCombiner : facetCombinerMap.values()) {
			builder.addFacetGroup(facetCombiner.getCombinedFacetGroup());
		}

		for (StatCombiner statCombiner : statCombinerMap.values()) {
			builder.addStatGroup(statCombiner.getCombinedStatGroupAndConvertToExternalType());
		}

		Map<String, ScoredResult[]> lastIndexResultMap = createLastIndexResultMapWithPreviousLastResults();
		List<ScoredResult> results;
		if (shardResponses.size() > 1) {
			results = mergeResults((int) returnedHits, resultsSize, lastIndexResultMap);
		}
		else {
			ShardQueryResponse shardQueryResponse = shardResponses.get(0);
			results = shardQueryResponse.getScoredResultList();
			if (!results.isEmpty()) {
				lastIndexResultMap.get(shardQueryResponse.getIndexName())[shardQueryResponse.getShardNumber()] = results.get(results.size() - 1);
			}
		}

		if (start == 0) {
			builder.addAllResults(results);
		}
		else {
			int i = 0;
			for (ScoredResult scoredResult : results) {
				if (i >= start) {
					builder.addResults(scoredResult);
				}
				i++;
			}
		}

		builder.setLastResult(createLastResult(lastIndexResultMap));

		return builder.build();
	}

	private List<ScoredResult> mergeResults(int returnedHits, int resultsSize, Map<String, ScoredResult[]> lastIndexResultMap) throws Exception {

		List<ScoredResult> results = Collections.emptyList();

		boolean sorting = (sortRequest != null && !sortRequest.getFieldSortList().isEmpty());

		List<ScoredResult> mergedResults = new ArrayList<>(returnedHits);
		for (ShardQueryResponse sr : shardResponses) {
			mergedResults.addAll(sr.getScoredResultList());
		}

		if (!mergedResults.isEmpty()) {

			List<FieldSort> fieldSortList = sortRequest != null ? sortRequest.getFieldSortList() : Collections.emptyList();
			HashMap<String, FieldConfig.FieldType> sortTypeMap = createSortTypeMap(fieldSortList);

			Comparator<ScoredResult> comparator = new ZuliaPostSortingComparator(fieldSortList, sortTypeMap);
			mergedResults.sort(comparator);

			results = mergedResults.subList(0, resultsSize);

			for (ScoredResult sr : results) {
				ScoredResult[] lastForShardArr = lastIndexResultMap.get(sr.getIndexName());
				lastForShardArr[sr.getShard()] = sr;
			}

			outside:
			for (ZuliaIndex index : indexes) {
				String indexName = index.getIndexName();
				ScoredResult[] lastForShardArr = lastIndexResultMap.get(indexName);
				ScoredResult lastForIndex = null;
				for (ScoredResult sr : lastForShardArr) {
					if (sr != null) {
						if (lastForIndex == null) {
							lastForIndex = sr;
						}
						else {
							if (comparator.compare(sr, lastForIndex) > 0) {
								lastForIndex = sr;
							}
						}
					}
				}

				if (lastForIndex == null) {
					//this happens when amount from index is zero
					continue;
				}

				double shardTolerance = index.getShardTolerance();

				int numberOfShards = index.getNumberOfShards();
				Map<Integer, ShardQueryResponse> shardResponseMap = indexToShardQueryResponseMap.get(indexName);
				for (int shardNumber = 0; shardNumber < numberOfShards; shardNumber++) {
					ShardQueryResponse sr = shardResponseMap.get(shardNumber);
					if (sr.hasNext()) {
						ScoredResult next = sr.getNext();
						int compare = comparator.compare(lastForIndex, next);
						if (compare > 0) {

							if (sorting) {
								String msg = "Result set did not return the most relevant sorted documents for index " + indexName + "\n";
								msg += "    Last for index from shard <" + lastForIndex.getShard() + "> has sort values <" + lastForIndex.getSortValues()
										+ ">\n";
								msg += "    Next for shard <" + next.getShard() + ">  has sort values <" + next.getSortValues() + ">\n";
								msg += "    Last for shards: \n";
								msg += "      " + Arrays.toString(lastForShardArr) + "\n";
								msg += "    Results: \n";
								msg += "      " + results + "\n";
								msg += "    If this happens frequently increase requestFactor or minShardRequest\n";
								msg += "    Retrying with full request.\n";
								LOG.error(msg);

								isShort = true;
								break outside;
							}

							double diff = (Math.abs(lastForIndex.getScore() - next.getScore()));
							if (diff > shardTolerance) {
								String msg = "Result set did not return the most relevant documents for index <" + indexName + "> with shard tolerance <"
										+ shardTolerance + ">\n";
								msg += "    Last for index from shard <" + lastForIndex.getShard() + "> has score <" + lastForIndex.getScore() + ">\n";
								msg += "    Next for shard <" + next.getShard() + "> has score <" + next.getScore() + ">\n";
								msg += "    Last for shards: \n";
								msg += "      " + Arrays.toString(lastForShardArr) + "\n";
								msg += "    Results: \n";
								msg += "      " + results + "\n";
								msg += "    If this happens frequently increase requestFactor, minShardRequest, or shardTolerance\n";
								msg += "    Retrying with full request.\n";
								LOG.error(msg);

								isShort = true;
								break outside;
							}
						}
					}
				}
			}

		}
		return results;
	}

	private HashMap<String, FieldConfig.FieldType> createSortTypeMap(List<FieldSort> fieldSortList) throws Exception {
		HashMap<String, FieldConfig.FieldType> sortTypeMap = new HashMap<>();

		for (FieldSort fieldSort : fieldSortList) {
			String sortField = fieldSort.getSortField();

			for (ZuliaIndex index : indexes) {
				FieldConfig.FieldType currentSortType = sortTypeMap.get(sortField);

				SortFieldInfo sortFieldInfo = index.getSortFieldType(sortField);
				FieldConfig.FieldType indexSortType = sortFieldInfo.getFieldType();

				if (currentSortType == null) {
					sortTypeMap.put(sortField, indexSortType);
				}
				else {
					if (!currentSortType.equals(indexSortType)) {
						String message = "Sort fields must be defined the same in all indexes searched in a single query.  Cannot sort on field " + sortField
								+ ": found type: " + currentSortType + " then type: " + indexSortType;
						LOG.error(message);
						throw new Exception(message);
					}
				}
			}
		}

		return sortTypeMap;
	}

	private LastResult createLastResult(Map<String, ScoredResult[]> lastIndexResultMap) {
		LastResult.Builder newLastResultBuilder = LastResult.newBuilder();
		for (String indexName : lastIndexResultMap.keySet()) {
			ScoredResult[] lastForShardArr = lastIndexResultMap.get(indexName);
			int numberOfShards = indexToShardCount.get(indexName);
			List<ScoredResult> indexList = new ArrayList<>();
			for (int shard = 0; shard < numberOfShards; shard++) {
				if (lastForShardArr[shard] != null) {
					ScoredResult.Builder minimalSR = ScoredResult.newBuilder(lastForShardArr[shard]).clearUniqueId().clearIndexName().clearResultIndex()
							.clearTimestamp().clearResultDocument();
					indexList.add(minimalSR.build());
				}
			}
			if (!indexList.isEmpty()) {
				LastIndexResult lastIndexResult = LastIndexResult.newBuilder().addAllLastForShard(indexList).setIndexName(indexName).build();
				newLastResultBuilder.addLastIndexResult(lastIndexResult);
			}
		}
		return newLastResultBuilder.build();
	}

	private Map<String, ScoredResult[]> createLastIndexResultMapWithPreviousLastResults() {
		Map<String, ScoredResult[]> lastIndexResultMap = new HashMap<>();

		for (String indexName : indexToShardQueryResponseMap.keySet()) {
			int numberOfShards = indexToShardCount.get(indexName);
			lastIndexResultMap.put(indexName, new ScoredResult[numberOfShards]);
		}

		for (LastIndexResult lir : lastResult.getLastIndexResultList()) {
			ScoredResult[] lastForShardArr = lastIndexResultMap.get(lir.getIndexName());
			// initialize with last results
			for (ScoredResult sr : lir.getLastForShardList()) {
				lastForShardArr[sr.getShard()] = sr;
			}
		}
		return lastIndexResultMap;
	}

	public boolean isShort() {
		return isShort;
	}

}
