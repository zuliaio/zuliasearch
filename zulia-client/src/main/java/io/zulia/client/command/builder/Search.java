package io.zulia.client.command.builder;

import io.zulia.client.command.base.MultiIndexRoutableCommand;
import io.zulia.client.command.base.SimpleCommand;
import io.zulia.client.command.factory.RangeFilter;
import io.zulia.client.pool.ZuliaConnection;
import io.zulia.client.result.SearchResult;
import io.zulia.message.ZuliaBase.MasterSlaveSettings;
import io.zulia.message.ZuliaBase.Similarity;
import io.zulia.message.ZuliaQuery;
import io.zulia.message.ZuliaQuery.FetchType;
import io.zulia.message.ZuliaQuery.FieldSimilarity;
import io.zulia.message.ZuliaQuery.LastResult;
import io.zulia.message.ZuliaServiceGrpc;
import io.zulia.message.ZuliaServiceOuterClass.QueryRequest;
import io.zulia.message.ZuliaServiceOuterClass.QueryResponse;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

public class Search extends SimpleCommand<QueryRequest, SearchResult> implements MultiIndexRoutableCommand {

	private final QueryRequest.Builder queryRequest = QueryRequest.newBuilder();
	private final ZuliaQuery.FacetRequest.Builder facetRequest = ZuliaQuery.FacetRequest.newBuilder();
	private final ZuliaQuery.SortRequest.Builder sortRequest = ZuliaQuery.SortRequest.newBuilder();

	public Search(String... indexes) {
		this(Arrays.asList(indexes));
	}

	public Search(Iterable<String> indexes) {
		setIndexes(indexes);
	}

	public Search setIndexes(Iterable<String> indexes) {

		for (String index : indexes) {
			if (index == null) {
				throw new IllegalArgumentException("Index cannot be null");
			}
		}

		queryRequest.clearIndex();
		queryRequest.addAllIndex(indexes);
		if (queryRequest.getIndexCount() == 0) {
			throw new IllegalArgumentException("Indexes must be given for a query");
		}
		return this;
	}

	public Search setIndexes(String... indexes) {
		return setIndexes(Arrays.asList(indexes));
	}

	public Search setIndex(String index) {
		return setIndexes(List.of(index));
	}

	/**
	 * @param rangeFilter - See {@link io.zulia.client.command.factory.FilterFactory}
	 * @return
	 */
	public Search addQuery(RangeFilter<?> rangeFilter) {
		return addQuery(rangeFilter.toQuery());
	}

	public Search addQuery(QueryBuilder queryBuilder) {
		queryRequest.addQuery(queryBuilder.getQuery());
		return this;
	}

	public void clearQueries() {
		queryRequest.clearQuery();
	}

	public int getAmount() {
		return queryRequest.getAmount();
	}

	public Search setAmount(int amount) {
		queryRequest.setAmount(amount);
		return this;
	}

	public boolean getDontCache() {
		return queryRequest.getDontCache();
	}

	public Search setDontCache(boolean dontCache) {
		queryRequest.setDontCache(dontCache);
		return this;
	}

	public boolean getPinToCache() {
		return queryRequest.getPinToCache();
	}

	public Search setPinToCache(boolean pinToCache) {
		queryRequest.setPinToCache(pinToCache);
		return this;
	}

	public boolean getDebug() {
		return queryRequest.getDebug();
	}

	public Search setDebug(boolean debug) {
		queryRequest.setDebug(debug);
		return this;
	}

	public int setStart() {
		return queryRequest.getStart();
	}

	public Search setStart(int start) {
		queryRequest.setStart(start);
		return this;
	}

	public List<String> getDocumentMaskedFields() {
		return queryRequest.getDocumentMaskedFieldsList();
	}

	public Search addDocumentMaskedField(String documentMaskedField) {
		queryRequest.addDocumentMaskedFields(documentMaskedField);
		return this;
	}

	public List<String> getDocumentFields() {
		return queryRequest.getDocumentFieldsList();
	}

	public Search addDocumentField(String documentField) {
		queryRequest.addDocumentFields(documentField);
		return this;
	}

	public Search addDocumentFields(String... documentFields) {
		queryRequest.addAllDocumentFields(List.of(documentFields));
		return this;
	}

	public Search addDocumentFields(Iterable<String> documentFields) {
		queryRequest.addAllDocumentFields(documentFields);
		return this;
	}

	public Search addFacetDrillDown(DrillDownBuilder drillDownBuilder) {
		facetRequest.addDrillDown(drillDownBuilder.getDrillDown());
		return this;
	}

	public List<ZuliaQuery.DrillDown> getFacetDrillDowns() {
		return facetRequest.getDrillDownList();
	}

	public FetchType getResultFetchType() {
		return queryRequest.getResultFetchType();
	}

	public Search setResultFetchType(FetchType resultFetchType) {
		queryRequest.setResultFetchType(resultFetchType);
		return this;
	}

	public LastResult getLastResult() {
		return queryRequest.getLastResult();
	}

	public Search setLastResult(SearchResult lastQueryResult) {
		return setLastResult(lastQueryResult.getLastResult());
	}

	public Search setLastResult(LastResult lastResult) {
		queryRequest.setLastResult(lastResult);
		return this;
	}

	public void clearLastResult() {
		queryRequest.clearLastResult();
	}

	public Search addFieldSimilarity(String field, Similarity similarity) {

		queryRequest.addFieldSimilarity(FieldSimilarity.newBuilder().setField(field).setSimilarity(similarity).build());

		return this;
	}

	public Search clearFieldSimilarity() {
		queryRequest.clearFieldSimilarity();
		return this;
	}

	public MasterSlaveSettings getMasterSlaveSettings() {
		return queryRequest.getMasterSlaveSettings();
	}

	public Search setMasterSlaveSettings(MasterSlaveSettings masterSlaveSettings) {
		queryRequest.setMasterSlaveSettings(masterSlaveSettings);
		return this;
	}

	public Search addHighlight(HighlightBuilder highlightBuilder) {
		queryRequest.addHighlightRequest(highlightBuilder.getHighlight());
		return this;
	}

	public Search clearHighlights() {
		queryRequest.clearHighlightRequest();
		return this;
	}

	public Search addSort(SortBuilder sortBuilder) {
		sortRequest.addFieldSort(sortBuilder.getSort());
		return this;
	}

	public Search clearSort() {
		sortRequest.clearFieldSort();
		return this;
	}

	public Search addCountFacet(CountFacetBuilder countFacetBuilder) {
		facetRequest.addCountRequest(countFacetBuilder.getFacetCount());
		return this;
	}

	public Search clearFacetCount() {
		facetRequest.clearCountRequest();
		return this;
	}

	public Search addStat(StatBuilder statBuilder) {
		facetRequest.addStatRequest(statBuilder.getStatRequest());
		return this;
	}

	public Search clearStat() {
		facetRequest.clearStatRequest();
		return this;
	}

	public Search addAnalysis(AnalysisBuilder analysisBuilder) {
		queryRequest.addAnalysisRequest(analysisBuilder.getAnalysis());
		return this;
	}

	public Search clearAnalysis() {
		queryRequest.clearAnalysisRequest();
		return this;
	}

	@Override
	public Collection<String> getIndexNames() {
		return queryRequest.getIndexList();
	}

	public Search setSearchLabel(String searchLabel) {
		queryRequest.setSearchLabel(searchLabel);
		return this;
	}

	public String getSearchLabel() {
		return queryRequest.getSearchLabel();
	}

	public Search setRealtime(boolean realtime) {
		queryRequest.setRealtime(realtime);
		return this;
	}

	public boolean getRealtime() {
		return queryRequest.getRealtime();
	}

	public Search setConcurrency(int concurrency) {
		queryRequest.setConcurrency(concurrency);
		return this;
	}

	public int getConcurrency() {
		return queryRequest.getConcurrency();
	}

	@Override
	public QueryRequest getRequest() {
		queryRequest.setFacetRequest(facetRequest);
		queryRequest.setSortRequest(sortRequest);
		return queryRequest.build();
	}

	@Override
	public SearchResult execute(ZuliaConnection zuliaConnection) {
		ZuliaServiceGrpc.ZuliaServiceBlockingStub service = zuliaConnection.getService();
		QueryResponse queryResponse = service.query(getRequest());
		return new SearchResult(queryResponse);
	}

	@Override
	public String toString() {
		return getRequest().toString();
	}
}
