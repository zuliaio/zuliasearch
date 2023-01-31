package io.zulia.server.search;

import io.zulia.ZuliaConstants;
import io.zulia.message.ZuliaBase;
import io.zulia.message.ZuliaQuery;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ShardQuery {
	Query query;
	Map<String, ZuliaBase.Similarity> similarityOverrideMap;
	int amount;
	Map<Integer, FieldDoc> shardToAfter;
	ZuliaQuery.FacetRequest facetRequest;
	ZuliaQuery.SortRequest sortRequest;
	QueryCacheKey queryCacheKey;
	ZuliaQuery.FetchType resultFetchType;
	List<String> fieldsToReturn;
	java.util.List<String> fieldsToMask;
	List<ZuliaQuery.HighlightRequest> highlightList;
	List<ZuliaQuery.AnalysisRequest> analysisRequestList;
	boolean debug;

	public ShardQuery(Query query, Map<String, ZuliaBase.Similarity> similarityOverrideMap, int amount, Map<Integer, FieldDoc> shardToAfter,
			ZuliaQuery.FacetRequest facetRequest, ZuliaQuery.SortRequest sortRequest, QueryCacheKey queryCacheKey, ZuliaQuery.FetchType resultFetchType,
			List<String> fieldsToReturn, List<String> fieldsToMask, List<ZuliaQuery.HighlightRequest> highlightList,
			List<ZuliaQuery.AnalysisRequest> analysisRequestList, boolean debug) {
		this.query = query;
		this.similarityOverrideMap = similarityOverrideMap;
		this.amount = amount;
		this.shardToAfter = shardToAfter;
		this.facetRequest = facetRequest;
		this.sortRequest = sortRequest;
		this.queryCacheKey = queryCacheKey;
		this.resultFetchType = resultFetchType;
		this.fieldsToReturn = fieldsToReturn;
		this.fieldsToMask = fieldsToMask;
		this.highlightList = highlightList;
		this.analysisRequestList = analysisRequestList;
		this.debug = debug;
	}

	public static ShardQuery queryById(String uniqueId, ZuliaQuery.FetchType resultFetchType, List<String> fieldsToReturn, List<String> fieldsToMask) {
		Query query = new TermQuery(new Term(ZuliaConstants.ID_FIELD, uniqueId));
		return new ShardQuery(query, null, 1, Collections.<Integer, FieldDoc>emptyMap(), null, null, null, resultFetchType, fieldsToReturn, fieldsToMask,
				Collections.emptyList(), Collections.emptyList(), false);
	}

	public Query getQuery() {
		return query;
	}

	public Map<String, ZuliaBase.Similarity> getSimilarityOverrideMap() {
		return similarityOverrideMap;
	}

	public int getAmount() {
		return amount;
	}

	public FieldDoc getAfter(int shardNumber) {
		return shardToAfter.get(shardNumber);
	}

	public ZuliaQuery.FacetRequest getFacetRequest() {
		return facetRequest;
	}

	public ZuliaQuery.SortRequest getSortRequest() {
		return sortRequest;
	}

	public QueryCacheKey getQueryCacheKey() {
		return queryCacheKey;
	}

	public ZuliaQuery.FetchType getResultFetchType() {
		return resultFetchType;
	}

	public List<String> getFieldsToReturn() {
		return fieldsToReturn;
	}

	public List<String> getFieldsToMask() {
		return fieldsToMask;
	}

	public List<ZuliaQuery.HighlightRequest> getHighlightList() {
		return highlightList;
	}

	public List<ZuliaQuery.AnalysisRequest> getAnalysisRequestList() {
		return analysisRequestList;
	}

	public boolean isDebug() {
		return debug;
	}
}
