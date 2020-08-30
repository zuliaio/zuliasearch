package io.zulia.client.command;

import io.zulia.client.command.base.MultiIndexRoutableCommand;
import io.zulia.client.command.base.SimpleCommand;
import io.zulia.client.pool.ZuliaConnection;
import io.zulia.client.result.QueryResult;
import io.zulia.message.ZuliaQuery;
import io.zulia.message.ZuliaQuery.FieldSort.Direction;
import io.zulia.message.ZuliaQuery.Query.QueryType;
import io.zulia.message.ZuliaServiceGrpc;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static io.zulia.message.ZuliaBase.Similarity;
import static io.zulia.message.ZuliaQuery.AnalysisRequest;
import static io.zulia.message.ZuliaQuery.CountRequest;
import static io.zulia.message.ZuliaQuery.Facet;
import static io.zulia.message.ZuliaQuery.FacetRequest;
import static io.zulia.message.ZuliaQuery.FetchType;
import static io.zulia.message.ZuliaQuery.FieldSimilarity;
import static io.zulia.message.ZuliaQuery.FieldSort;
import static io.zulia.message.ZuliaQuery.HighlightRequest;
import static io.zulia.message.ZuliaQuery.LastResult;
import static io.zulia.message.ZuliaQuery.Query.Operator;
import static io.zulia.message.ZuliaQuery.SortRequest;
import static io.zulia.message.ZuliaServiceOuterClass.QueryRequest;
import static io.zulia.message.ZuliaServiceOuterClass.QueryResponse;

/**
 * Runs a query on one of more Zulia indexes.
 * @author mdavis
 *
 */
public class Query extends SimpleCommand<QueryRequest, QueryResult> implements MultiIndexRoutableCommand {

	private String query;
	private int amount;
	private int start;
	private Collection<String> indexes;
	private LastResult lastResult;
	private List<CountRequest> countRequests = Collections.emptyList();
	private List<Facet> drillDowns = Collections.emptyList();
	private List<FieldSort> fieldSorts = Collections.emptyList();
	private Set<String> queryFields = Collections.emptySet();
	private List<ZuliaQuery.Query.Builder> filterQueries = Collections.emptyList();
	private List<ZuliaQuery.Query.Builder> scoredQueries = Collections.emptyList();
	private List<ZuliaQuery.Query.Builder> termQueries = Collections.emptyList();
	private List<ZuliaQuery.Query.Builder> cosineSimQueries = Collections.emptyList();
	private List<ZuliaQuery.HighlightRequest> highlightRequests = Collections.emptyList();
	private List<ZuliaQuery.AnalysisRequest> analysisRequests = Collections.emptyList();
	private Integer minimumNumberShouldMatch;
	private Operator defaultOperator;
	private FetchType resultFetchType;
	private Set<String> documentFields = Collections.emptySet();
	private Set<String> documentMaskedFields = Collections.emptySet();
	private List<FieldSimilarity> fieldSimilarities = Collections.emptyList();

	private Boolean dismax;
	private Float dismaxTie;
	private Boolean dontCache;
	private Boolean debug;

	public Query(String index, String query, int amount) {
		this(new String[] { index }, query, amount);
	}

	public Query(String[] indexes, String query, int amount) {
		this(new ArrayList<>(Arrays.asList(indexes)), query, amount);
	}

	public Query(Collection<String> indexes, String query, int amount) {
		this.indexes = indexes;
		this.query = query;
		this.amount = amount;
	}

	public Float getDismaxTie() {
		return dismaxTie;
	}

	public Query setDismaxTie(Float dismaxTie) {
		this.dismaxTie = dismaxTie;
		return this;
	}

	public Boolean getDismax() {
		return dismax;
	}

	public Query setDismax(Boolean dismax) {
		this.dismax = dismax;
		return this;
	}

	public String getQuery() {
		return query;
	}

	public Query setQuery(String query) {
		this.query = query;
		return this;
	}

	public int getAmount() {
		return amount;
	}

	public Query setAmount(int amount) {
		this.amount = amount;
		return this;
	}

	public int getStart() {
		return start;
	}

	public Query setStart(int start) {
		this.start = start;
		return this;
	}

	public Integer getMinimumNumberShouldMatch() {
		return minimumNumberShouldMatch;
	}

	public void setMinimumNumberShouldMatch(Integer minimumNumberShouldMatch) {
		this.minimumNumberShouldMatch = minimumNumberShouldMatch;
	}

	public Collection<String> getIndexes() {
		return indexes;
	}

	public Query setIndexes(Collection<String> indexes) {
		this.indexes = indexes;
		return this;
	}

	public Query setLastResult(QueryResult lastQueryResult) {
		this.lastResult = lastQueryResult.getLastResult();
		return this;
	}

	public LastResult getLastResult() {
		return lastResult;
	}

	public Query setLastResult(LastResult lastResult) {
		this.lastResult = lastResult;
		return this;
	}

	public Query addDrillDown(String label, String path) {
		if (drillDowns.isEmpty()) {
			this.drillDowns = new ArrayList<>();
		}

		drillDowns.add(Facet.newBuilder().setLabel(label).setValue(path).build());
		return this;
	}

	public List<Facet> getDrillDowns() {
		return drillDowns;
	}

	public Set<String> getQueryFields() {
		return queryFields;
	}

	public Query setQueryFields(String... queryFields) {
		this.queryFields = new HashSet<>(Arrays.asList(queryFields));
		return this;

	}

	public Query setQueryFields(Collection<String> queryFields) {
		this.queryFields = new HashSet<String>(queryFields);
		return this;
	}

	public Query addFieldSimilarity(String field, Similarity similarity) {

		FieldSimilarity fieldSimilarity = FieldSimilarity.newBuilder().setField(field).setSimilarity(similarity).build();

		return addFieldSimilarity(fieldSimilarity);
	}

	private Query addFieldSimilarity(FieldSimilarity fieldSimilarity) {
		if (fieldSimilarities.isEmpty()) {
			fieldSimilarities = new ArrayList<>();
		}

		fieldSimilarities.add(fieldSimilarity);

		return this;
	}

	public Query addTermQuery(Iterable<String> terms, String... field) {

		ZuliaQuery.Query.Builder builder = ZuliaQuery.Query.newBuilder().addAllQf(Arrays.asList(field)).addAllTerm(terms);
		builder.setQueryType(QueryType.TERMS);
		addTermQuery(builder);
		return this;
	}

	private Query addTermQuery(ZuliaQuery.Query.Builder termQuery) {
		if (termQueries.isEmpty()) {
			termQueries = new ArrayList<>();
		}
		termQueries.add(termQuery);

		return this;
	}

	public Query addCosineSim(double[] vector, double similarity, String... field) {

		ZuliaQuery.Query.Builder builder = ZuliaQuery.Query.newBuilder().addAllQf(Arrays.asList(field)).setVectorSimilarity(similarity);
		for (int i = 0; i < vector.length; i++) {
			builder.addVector(vector[i]);
		}
		addCosineSim(builder);
		return this;
	}

	private Query addCosineSim(ZuliaQuery.Query.Builder cosineSimQuery) {
		if (cosineSimQueries.isEmpty()) {
			cosineSimQueries = new ArrayList<>();
		}
		cosineSimQuery.setQueryType(QueryType.VECTOR);
		cosineSimQueries.add(cosineSimQuery);

		return this;
	}

	public Query addQueryField(String queryField) {
		if (queryFields.isEmpty()) {
			this.queryFields = new HashSet<>();
		}

		queryFields.add(queryField);
		return this;
	}

	public Query addQueryField(String... queryFields) {
		if (this.queryFields.isEmpty()) {
			this.queryFields = new HashSet<>();
		}

		for (String queryField : queryFields) {
			this.queryFields.add(queryField);
		}
		return this;
	}

	public List<ZuliaQuery.Query.Builder> getFilterQueries() {
		return filterQueries;
	}

	public void setFilterQueries(List<ZuliaQuery.Query.Builder> filterQueries) {
		this.filterQueries = filterQueries;
	}

	public Query addFilterQuery(String query) {
		return addFilterQuery(query, null, null, null);
	}

	public Query addFilterQuery(String query, Collection<String> queryFields) {
		return addFilterQuery(query, queryFields, null, null);
	}

	public Query addFilterQuery(String query, Collection<String> queryFields, Operator defaultOperator) {
		return addFilterQuery(query, queryFields, defaultOperator, null);
	}

	public Query addFilterQuery(String query, Collection<String> queryFields, Integer minimumNumberShouldMatch) {
		return addFilterQuery(query, queryFields, null, minimumNumberShouldMatch);
	}

	public Query addFilterQuery(String query, Collection<String> queryFields, Operator defaultOperator, Integer minimumNumberShouldMatch) {
		if (filterQueries.isEmpty()) {
			this.filterQueries = new ArrayList<>();
		}

		ZuliaQuery.Query.Builder builder = ZuliaQuery.Query.newBuilder();
		builder.setQueryType(QueryType.FILTER);
		fillInQuery(query, queryFields, defaultOperator, minimumNumberShouldMatch, builder);
		filterQueries.add(builder);
		return this;
	}

	private void fillInQuery(String query, Collection<String> queryFields, Operator defaultOperator, Integer minimumNumberShouldMatch,
			ZuliaQuery.Query.Builder builder) {
		if (query != null && !query.isEmpty()) {
			builder.setQ(query);
		}
		if (minimumNumberShouldMatch != null) {
			builder.setMm(minimumNumberShouldMatch);
		}
		if (defaultOperator != null) {
			builder.setDefaultOp(defaultOperator);
		}
		if (queryFields != null && !queryFields.isEmpty()) {
			builder.addAllQf(queryFields);
		}
	}

	public List<ZuliaQuery.Query.Builder> getScoredQueries() {
		return scoredQueries;
	}

	public void setScoredQueries(List<ZuliaQuery.Query.Builder> scoredQueries) {
		this.scoredQueries = scoredQueries;
	}

	public Query addScoredQuery(String query) {
		return addScoredQuery(query, null, null, null, true);
	}

	public Query addScoredQuery(String query, Collection<String> queryFields) {
		return addScoredQuery(query, queryFields, null, null, true);
	}

	public Query addScoredQuery(String query, Collection<String> queryFields, Operator defaultOperator) {
		return addScoredQuery(query, queryFields, defaultOperator, null, true);
	}

	public Query addScoredQuery(String query, Collection<String> queryFields, Integer minimumNumberShouldMatch) {
		return addScoredQuery(query, queryFields, null, minimumNumberShouldMatch, true);
	}

	public Query addScoredQuery(String query, Collection<String> queryFields, Integer minimumNumberShouldMatch, boolean must) {
		return addScoredQuery(query, queryFields, null, minimumNumberShouldMatch, must);
	}

	public Query addScoredQuery(String query, Collection<String> queryFields, Operator defaultOperator, Integer minimumNumberShouldMatch, boolean must) {
		if (scoredQueries.isEmpty()) {
			this.scoredQueries = new ArrayList<>();
		}

		ZuliaQuery.Query.Builder builder = ZuliaQuery.Query.newBuilder();
		builder.setQueryType(must ? QueryType.SCORE_MUST : QueryType.SCORE_SHOULD);
		fillInQuery(query, queryFields, defaultOperator, minimumNumberShouldMatch, builder);
		scoredQueries.add(builder);
		return this;
	}

	public Query addHighlight(String field) {
		return addHighlight(field, null, null, null);
	}

	public Query addHighlight(String field, Integer numberOfFragments) {
		return addHighlight(field, null, null, numberOfFragments);
	}

	public Query addHighlight(String field, String preTag, String postTag, Integer numberOfFragments) {
		if (highlightRequests.isEmpty()) {
			this.highlightRequests = new ArrayList<>();
		}

		HighlightRequest.Builder highlight = HighlightRequest.newBuilder();
		if (field != null && !field.isEmpty()) {
			highlight.setField(field);
		}
		if (preTag != null) {
			highlight.setPreTag(preTag);
		}
		if (postTag != null) {
			highlight.setPostTag(postTag);
		}
		if (numberOfFragments != null) {
			highlight.setNumberOfFragments(numberOfFragments);
		}

		highlightRequests.add(highlight.build());
		return this;
	}

	public Query addSummaryAnalysis(String field, int topN) {
		return addAnalysis(field, false, false, true, topN);
	}

	public Query addAnalysis(String field, Boolean tokens, Boolean docTerms, Boolean summaryTerms, Integer topN) {

		AnalysisRequest.Builder analysisRequest = AnalysisRequest.newBuilder();
		if (field != null && !field.isEmpty()) {
			analysisRequest.setField(field);
		}
		if (tokens != null) {
			analysisRequest.setTokens(tokens);
		}
		if (docTerms != null) {
			analysisRequest.setDocTerms(docTerms);
		}
		if (summaryTerms != null) {
			analysisRequest.setSummaryTerms(summaryTerms);
		}
		if (topN != null) {
			analysisRequest.setTopN(topN);
		}

		return addAnalysis(analysisRequest);
	}

	public Query addAnalysis(AnalysisRequest.Builder analysisRequest) {
		if (analysisRequests.isEmpty()) {
			this.analysisRequests = new ArrayList<>();
		}
		analysisRequests.add(analysisRequest.build());
		return this;
	}

	public Query addCountRequest(String label) {
		return (addCountRequest(label, null));
	}

	public Query addCountRequest(String label, Integer maxFacets) {
		return (addCountRequest(label, maxFacets, null));
	}

	public Query addCountRequest(String label, Integer maxFacets, Integer shardFacets) {

		CountRequest.Builder countRequest = CountRequest.newBuilder().setFacetField(Facet.newBuilder().setLabel(label).build());
		if (maxFacets != null) {
			countRequest.setMaxFacets(maxFacets);
		}
		if (shardFacets != null) {
			countRequest.setShardFacets(shardFacets);
		}
		if (countRequests.isEmpty()) {
			this.countRequests = new ArrayList<>();
		}
		countRequests.add(countRequest.build());
		return this;
	}

	public List<CountRequest> getCountRequests() {
		return countRequests;
	}

	public Query addFieldSort(String sort) {
		if (fieldSorts.isEmpty()) {
			this.fieldSorts = new ArrayList<>();
		}
		fieldSorts.add(FieldSort.newBuilder().setSortField(sort).setDirection(Direction.ASCENDING).build());
		return this;
	}

	public Query addFieldSort(String sort, Direction direction) {
		if (fieldSorts.isEmpty()) {
			this.fieldSorts = new ArrayList<>();
		}
		fieldSorts.add(FieldSort.newBuilder().setSortField(sort).setDirection(direction).build());
		return this;
	}

	public Query addFieldSort(String sort, Direction direction, boolean missingLast) {
		if (fieldSorts.isEmpty()) {
			this.fieldSorts = new ArrayList<>();
		}
		fieldSorts.add(FieldSort.newBuilder().setSortField(sort).setDirection(direction).setMissingLast(missingLast).build());
		return this;
	}

	public Operator getDefaultOperator() {
		return defaultOperator;
	}

	public Query setDefaultOperator(Operator defaultOperator) {
		this.defaultOperator = defaultOperator;
		return this;
	}

	public FetchType getResultFetchType() {
		return resultFetchType;
	}

	public Query setResultFetchType(FetchType resultFetchType) {
		this.resultFetchType = resultFetchType;
		return this;
	}

	public Set<String> getDocumentMaskedFields() {
		return documentMaskedFields;
	}

	public Query addDocumentMaskedField(String documentMaskedField) {
		if (documentMaskedFields.isEmpty()) {
			documentMaskedFields = new LinkedHashSet<>();
		}

		documentMaskedFields.add(documentMaskedField);
		return this;
	}

	public Set<String> getDocumentFields() {
		return documentFields;
	}

	public Query addDocumentField(String documentField) {
		if (documentFields.isEmpty()) {
			this.documentFields = new LinkedHashSet<>();
		}
		documentFields.add(documentField);
		return this;
	}

	public Boolean getDontCache() {
		return dontCache;
	}

	public void setDontCache(Boolean dontCache) {
		this.dontCache = dontCache;
	}

	public Boolean getDebug() {
		return debug;
	}

	public void setDebug(Boolean debug) {
		this.debug = debug;
	}

	@Override
	public Collection<String> getIndexNames() {
		return indexes;
	}

	@Override
	public QueryRequest getRequest() {
		QueryRequest.Builder requestBuilder = QueryRequest.newBuilder();
		requestBuilder.setAmount(amount);
		requestBuilder.setStart(start);

		if (debug != null) {
			requestBuilder.setDebug(debug);
		}

		ZuliaQuery.Query.Builder mainQueryBuilder = ZuliaQuery.Query.newBuilder();
		if (query != null) {
			mainQueryBuilder.setQ(query);
		}
		if (minimumNumberShouldMatch != null) {
			mainQueryBuilder.setMm(minimumNumberShouldMatch);
		}
		if (!queryFields.isEmpty()) {
			mainQueryBuilder.addAllQf(queryFields);
		}
		if (defaultOperator != null) {
			mainQueryBuilder.setDefaultOp(defaultOperator);
		}

		if (dismax != null) {
			mainQueryBuilder.setDismax(dismax);
			if (dismaxTie != null) {
				mainQueryBuilder.setDismaxTie(dismaxTie);
			}
		}

		mainQueryBuilder.setQueryType(QueryType.SCORE_MUST);
		requestBuilder.addQuery(mainQueryBuilder);

		if (lastResult != null) {
			requestBuilder.setLastResult(lastResult);
		}

		if (!fieldSimilarities.isEmpty()) {
			requestBuilder.addAllFieldSimilarity(fieldSimilarities);
		}

		for (String index : indexes) {
			requestBuilder.addIndex(index);
		}

		if (!drillDowns.isEmpty() || !countRequests.isEmpty()) {
			FacetRequest.Builder facetRequestBuilder = FacetRequest.newBuilder();

			facetRequestBuilder.addAllDrillDown(drillDowns);

			facetRequestBuilder.addAllCountRequest(countRequests);

			requestBuilder.setFacetRequest(facetRequestBuilder.build());

		}

		for (ZuliaQuery.Query.Builder filterQuery : filterQueries) {
			requestBuilder.addQuery(filterQuery.setQueryType(QueryType.FILTER));
		}

		for (ZuliaQuery.Query.Builder cosineSimQuery : cosineSimQueries) {
			requestBuilder.addQuery(cosineSimQuery.setQueryType(QueryType.VECTOR));
		}

		for (ZuliaQuery.Query.Builder termQuery : termQueries) {
			requestBuilder.addQuery(termQuery.setQueryType(QueryType.TERMS));
		}

		for (ZuliaQuery.Query.Builder scoredQuery : scoredQueries) {
			requestBuilder.addQuery(scoredQuery);
		}

		if (!highlightRequests.isEmpty()) {
			requestBuilder.addAllHighlightRequest(highlightRequests);
		}

		if (!analysisRequests.isEmpty()) {
			requestBuilder.addAllAnalysisRequest(analysisRequests);
		}

		if (resultFetchType != null) {
			requestBuilder.setResultFetchType(resultFetchType);
		}
		if (dontCache != null) {
			requestBuilder.setDontCache(dontCache);
		}

		requestBuilder.addAllDocumentFields(documentFields);
		requestBuilder.addAllDocumentMaskedFields(documentMaskedFields);

		SortRequest.Builder sortRequestBuilder = SortRequest.newBuilder();
		sortRequestBuilder.addAllFieldSort(fieldSorts);
		requestBuilder.setSortRequest(sortRequestBuilder.build());

		return requestBuilder.build();
	}

	@Override
	public QueryResult execute(ZuliaConnection zuliaConnection) {

		ZuliaServiceGrpc.ZuliaServiceBlockingStub service = zuliaConnection.getService();

		QueryResponse queryResponse = service.query(getRequest());

		return new QueryResult(queryResponse);

	}

	@Override
	public String toString() {
		return "Query{" + "query='" + query + '\'' + ", amount=" + amount + ", indexes=" + indexes + ", lastResult=" + lastResult + ", countRequests="
				+ countRequests + ", drillDowns=" + drillDowns + ", fieldSorts=" + fieldSorts + ", queryFields=" + queryFields + ", filterQueries="
				+ filterQueries + ", minimumNumberShouldMatch=" + minimumNumberShouldMatch + ", defaultOperator=" + defaultOperator + ", resultFetchType="
				+ resultFetchType + ", documentFields=" + documentFields + ", documentMaskedFields=" + documentMaskedFields + '}';
	}

}
