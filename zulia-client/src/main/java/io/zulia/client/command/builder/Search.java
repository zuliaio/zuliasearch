package io.zulia.client.command.builder;

import io.zulia.client.command.base.MultiIndexRoutableCommand;
import io.zulia.client.command.base.SimpleCommand;
import io.zulia.client.pool.ZuliaConnection;
import io.zulia.client.result.SearchResult;
import io.zulia.message.ZuliaBase.MasterSlaveSettings;
import io.zulia.message.ZuliaBase.Similarity;
import io.zulia.message.ZuliaQuery;
import io.zulia.message.ZuliaQuery.Facet;
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
		queryRequest.addAllIndex(indexes);
		if (queryRequest.getIndexCount() == 0) {
			throw new IllegalArgumentException("Indexes must be given for a query");
		}
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

	public Search addFacetDrillDown(String label, String path) {
		facetRequest.addDrillDown(Facet.newBuilder().setLabel(label).setValue(path).build());
		return this;
	}

	public List<Facet> getFacetDrillDowns() {
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
		return queryRequest.toString();
	}
}
