package io.zulia.server.search;

import io.zulia.ZuliaConstants;
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
import io.zulia.message.ZuliaQuery.SortValues;
import io.zulia.message.ZuliaServiceOuterClass.InternalQueryResponse;
import io.zulia.message.ZuliaServiceOuterClass.QueryRequest;
import io.zulia.message.ZuliaServiceOuterClass.QueryResponse;
import io.zulia.server.analysis.frequency.TermFreq;
import io.zulia.server.field.FieldTypeUtil;
import io.zulia.server.index.ZuliaIndex;
import org.apache.lucene.util.BytesRef;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

public class QueryCombiner {

	private final static Comparator<ScoredResult> scoreCompare = new ScoreCompare();
	private final static Comparator<ScoredResult> reverseScoreCompare = new ReverseScoreCompare();

	private final static Logger log = Logger.getLogger(QueryCombiner.class.getSimpleName());

	private final List<InternalQueryResponse> responses;

	private final Map<String, Map<Integer, ShardQueryResponse>> indexToShardQueryResponseMap;
	private final List<ShardQueryResponse> shardResponses;

	private final int amount;
	private final int start;
	private final LastResult lastResult;
	private final List<AnalysisRequest> analysisRequestList;

	private boolean isShort;
	private List<ScoredResult> results;
	private int resultsSize;

	private SortRequest sortRequest;

	private final Collection<ZuliaIndex> indexes;
	private final Map<String, Integer> indexToShardCount;

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
		this.results = Collections.emptyList();
		this.resultsSize = 0;
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
						throw new Exception("Shard <" + shardNumber + "> is repeated for <" + indexName + ">");
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
				throw new Exception("Missing index <" + index.getIndexName() + "> in response");
			}

			if (shardResponseMap.size() != numberOfShards) {
				throw new Exception("Found <" + shardResponseMap.size() + "> expected <" + numberOfShards + ">");
			}

			for (int shardNumber = 0; shardNumber < numberOfShards; shardNumber++) {
				if (!shardResponseMap.containsKey(shardNumber)) {
					throw new Exception("Missing shard <" + shardNumber + ">");
				}
			}
		}
	}

	public QueryResponse getQueryResponse() throws Exception {

		validate();

		boolean sorting = (sortRequest != null && !sortRequest.getFieldSortList().isEmpty());

		long totalHits = 0;
		long returnedHits = 0;
		for (ShardQueryResponse sr : shardResponses) {
			totalHits += sr.getTotalHits();
			returnedHits += sr.getScoredResultList().size();
		}

		QueryResponse.Builder builder = QueryResponse.newBuilder();
		builder.setTotalHits(totalHits);

		resultsSize = Math.min(amount, (int) returnedHits);

		results = Collections.emptyList();

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

		Map<CountRequest, FacetCombiner> facetCombinerMap = new HashMap<>();

		Map<AnalysisRequest, Map<String, Term.Builder>> analysisRequestToTermMap = new HashMap<>();

		int shardIndex = 0;

		for (ShardQueryResponse sr : shardResponses) {
			for (FacetGroup fg : sr.getFacetGroupList()) {
				CountRequest countRequest = fg.getCountRequest();
				FacetCombiner facetCombiner = facetCombinerMap.computeIfAbsent(countRequest,
						countRequest1 -> new FacetCombiner(countRequest, shardResponses.size()));
				facetCombiner.handleFacetGroupForShard(fg, shardIndex);
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

		List<ScoredResult> mergedResults = new ArrayList<>((int) returnedHits);
		for (ShardQueryResponse sr : shardResponses) {
			mergedResults.addAll(sr.getScoredResultList());
		}

		Comparator<ScoredResult> myCompare = scoreCompare;

		if (sorting) {
			final List<FieldSort> fieldSortList = sortRequest.getFieldSortList();

			final HashMap<String, FieldConfig.FieldType> sortTypeMap = new HashMap<>();

			for (FieldSort fieldSort : fieldSortList) {
				String sortField = fieldSort.getSortField();

				if (ZuliaQueryParser.rewriteLengthFields(sortField).equals(sortField)) {

					for (ZuliaIndex index : indexes) {
						FieldConfig.FieldType currentSortType = sortTypeMap.get(sortField);

						FieldConfig.FieldType indexSortType = index.getSortFieldType(sortField);
						if (currentSortType == null) {
							sortTypeMap.put(sortField, indexSortType);
						}
						else {
							if (!currentSortType.equals(indexSortType)) {
								log.severe("Sort fields must be defined the same in all indexes searched in a single query");
								String message =
										"Cannot sort on field <" + sortField + ">: found type: <" + currentSortType + "> then type: <" + indexSortType + ">";
								log.severe(message);

								throw new Exception(message);
							}
						}
					}
				}
			}

			myCompare = (o1, o2) -> {
				int compare = 0;

				int sortValueIndex = 0;

				SortValues sortValues1 = o1.getSortValues();
				SortValues sortValues2 = o2.getSortValues();
				for (FieldSort fs : fieldSortList) {
					String sortField = fs.getSortField();

					FieldConfig.FieldType sortType = sortTypeMap.get(sortField);

					if (!ZuliaQueryParser.rewriteLengthFields(sortField).equals(sortField)) {
						sortType = FieldConfig.FieldType.NUMERIC_LONG;
					}

					if (ZuliaConstants.SCORE_FIELD.equals(sortField)) {
						if (FieldSort.Direction.DESCENDING.equals(fs.getDirection())) {
							compare = scoreCompare.compare(o1, o2);
						}
						else {
							compare = reverseScoreCompare.compare(o1, o2);
						}
					}
					else {
						ZuliaQuery.SortValue sortValue1 = sortValues1.getSortValue(sortValueIndex);
						ZuliaQuery.SortValue sortValue2 = sortValues2.getSortValue(sortValueIndex);

						if (FieldTypeUtil.isNumericIntFieldType(sortType)) {
							Integer a = sortValue1.getExists() ? sortValue1.getIntegerValue() : null;
							Integer b = sortValue2.getExists() ? sortValue2.getIntegerValue() : null;

							if (!fs.getMissingLast()) {
								compare = Comparator.nullsFirst(Integer::compareTo).compare(a, b);
							}
							else {
								compare = Comparator.nullsLast(Integer::compareTo).compare(a, b);
							}
						}
						else if (FieldTypeUtil.isNumericLongFieldType(sortType)) {
							Long a = sortValue1.getExists() ? sortValue1.getLongValue() : null;
							Long b = sortValue2.getExists() ? sortValue2.getLongValue() : null;

							if (!fs.getMissingLast()) {
								compare = Comparator.nullsFirst(Long::compareTo).compare(a, b);
							}
							else {
								compare = Comparator.nullsLast(Long::compareTo).compare(a, b);
							}
						}
						else if (FieldTypeUtil.isDateFieldType(sortType)) {
							Long a = sortValue1.getExists() ? sortValue1.getDateValue() : null;
							Long b = sortValue2.getExists() ? sortValue2.getDateValue() : null;

							if (!fs.getMissingLast()) {
								compare = Comparator.nullsFirst(Long::compareTo).compare(a, b);
							}
							else {
								compare = Comparator.nullsLast(Long::compareTo).compare(a, b);
							}
						}
						else if (FieldTypeUtil.isNumericFloatFieldType(sortType)) {

							Float a = sortValue1.getExists() ? sortValue1.getFloatValue() : null;
							Float b = sortValue2.getExists() ? sortValue2.getFloatValue() : null;

							if (!fs.getMissingLast()) {
								compare = Comparator.nullsFirst(Float::compareTo).compare(a, b);
							}
							else {
								compare = Comparator.nullsLast(Float::compareTo).compare(a, b);
							}
						}
						else if (FieldTypeUtil.isNumericDoubleFieldType(sortType)) {

							Double a = sortValue1.getExists() ? sortValue1.getDoubleValue() : null;
							Double b = sortValue2.getExists() ? sortValue2.getDoubleValue() : null;

							if (!fs.getMissingLast()) {
								compare = Comparator.nullsFirst(Double::compareTo).compare(a, b);
							}
							else {
								compare = Comparator.nullsLast(Double::compareTo).compare(a, b);
							}

						}
						else {
							String a = sortValue1.getExists() ? sortValue1.getStringValue() : null;
							String b = sortValue2.getExists() ? sortValue2.getStringValue() : null;

							if (!fs.getMissingLast()) {
								compare = Comparator.nullsFirst(BytesRef::compareTo)
										.compare(a != null ? new BytesRef(a) : null, b != null ? new BytesRef(b) : null);
							}
							else {
								compare = Comparator.nullsLast(BytesRef::compareTo)
										.compare(a != null ? new BytesRef(a) : null, b != null ? new BytesRef(b) : null);
							}
						}

						if (FieldSort.Direction.DESCENDING.equals(fs.getDirection())) {
							compare *= -1;
						}
					}

					if (compare != 0) {
						return compare;
					}

					sortValueIndex++;

				}

				return compare;
			};
		}

		if (!mergedResults.isEmpty()) {
			mergedResults.sort(myCompare);

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
							if (myCompare.compare(sr, lastForIndex) > 0) {
								lastForIndex = sr;
							}
						}
					}
				}

				if (lastForIndex == null) {
					//this happen when amount from index is zero
					continue;
				}

				double shardTolerance = index.getShardTolerance();

				int numberOfShards = index.getNumberOfShards();
				Map<Integer, ShardQueryResponse> shardResponseMap = indexToShardQueryResponseMap.get(indexName);
				for (int shardNumber = 0; shardNumber < numberOfShards; shardNumber++) {
					ShardQueryResponse sr = shardResponseMap.get(shardNumber);
					if (sr.hasNext()) {
						ScoredResult next = sr.getNext();
						int compare = myCompare.compare(lastForIndex, next);
						if (compare > 0) {

							if (sorting) {
								String msg = "Result set did not return the most relevant sorted documents for index <" + indexName + ">\n";
								msg += "    Last for index from shard <" + lastForIndex.getShard() + "> has sort values <" + lastForIndex.getSortValues()
										+ ">\n";
								msg += "    Next for shard <" + next.getShard() + ">  has sort values <" + next.getSortValues() + ">\n";
								msg += "    Last for shards: \n";
								msg += "      " + Arrays.toString(lastForShardArr) + "\n";
								msg += "    Results: \n";
								msg += "      " + results + "\n";
								msg += "    If this happens frequently increase requestFactor or minShardRequest\n";
								msg += "    Retrying with full request.\n";
								log.severe(msg);

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
								log.severe(msg);

								isShort = true;
								break outside;
							}
						}
					}
				}
			}

		}

		int i = 0;
		for (ScoredResult scoredResult : results) {
			if (i >= start) {
				builder.addResults(scoredResult);
			}
			i++;
		}

		LastResult.Builder newLastResultBuilder = LastResult.newBuilder();
		for (String indexName : lastIndexResultMap.keySet()) {
			ScoredResult[] lastForShardArr = lastIndexResultMap.get(indexName);
			int numberOfShards = indexToShardCount.get(indexName);
			List<ScoredResult> indexList = new ArrayList<>();
			for (int shard = 0; shard < numberOfShards; shard++) {
				if (lastForShardArr[shard] != null) {
					ScoredResult.Builder minimalSR = ScoredResult.newBuilder(lastForShardArr[shard]);
					minimalSR = minimalSR.clearUniqueId().clearIndexName().clearResultIndex().clearTimestamp().clearResultDocument();
					indexList.add(minimalSR.build());
				}
			}
			if (!indexList.isEmpty()) {
				LastIndexResult lastIndexResult = LastIndexResult.newBuilder().addAllLastForShard(indexList).setIndexName(indexName).build();
				newLastResultBuilder.addLastIndexResult(lastIndexResult);
			}
		}

		builder.setLastResult(newLastResultBuilder.build());

		return builder.build();
	}

	public boolean isShort() {
		return isShort;
	}

}
