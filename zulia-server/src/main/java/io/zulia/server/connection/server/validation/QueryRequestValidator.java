package io.zulia.server.connection.server.validation;

import io.zulia.message.ZuliaQuery.AnalysisRequest;
import io.zulia.message.ZuliaQuery.HighlightRequest;
import io.zulia.message.ZuliaQuery.Query;
import io.zulia.message.ZuliaQuery.SortRequest;
import io.zulia.message.ZuliaServiceOuterClass.QueryRequest;

import static io.zulia.message.ZuliaQuery.CountRequest;
import static io.zulia.message.ZuliaQuery.FacetRequest;
import static io.zulia.message.ZuliaQuery.FieldSort;

/**
 * Created by Payam Meyer on 9/19/17.
 * @author pmeyer
 */
public class QueryRequestValidator implements DefaultValidator<QueryRequest> {

	@Override
	public QueryRequest validateAndSetDefault(QueryRequest request) {
		QueryRequest.Builder queryRequestBuilder = request.toBuilder();

		Query.Builder queryBuilder = queryRequestBuilder.getQuery().toBuilder();

		if (queryBuilder.getDefaultOp() == null) {
			queryBuilder.setDefaultOp(Query.Operator.OR);
		}

		FacetRequest.Builder facetRequestBuilder = queryRequestBuilder.getFacetRequest().toBuilder();

		for (CountRequest countRequest : facetRequestBuilder.getCountRequestList()) {

			CountRequest.Builder countRequestBuilder = countRequest.toBuilder();

			if (countRequestBuilder.getMaxFacets() == 0) {
				countRequestBuilder.setMaxFacets(10);
			}

		}

		SortRequest.Builder sortRequestBuilder = queryRequestBuilder.getSortRequest().toBuilder();

		for (FieldSort fieldSort : sortRequestBuilder.getFieldSortList()) {

			FieldSort.Builder fieldSortBuilder = fieldSort.toBuilder();

			if (fieldSortBuilder.getDirection() == null) {
				fieldSortBuilder.setDirection(FieldSort.Direction.ASCENDING);
			}

		}

		if (queryRequestBuilder.getHighlightRequestList() != null) {

			for (HighlightRequest highlightRequest : queryRequestBuilder.getHighlightRequestList()) {

				HighlightRequest.Builder highlightRequestBuilder = highlightRequest.toBuilder();

				if (highlightRequestBuilder.getPreTag() == null) {
					highlightRequestBuilder.setPreTag("<em>");
				}

				if (highlightRequestBuilder.getPostTag() == null) {
					highlightRequestBuilder.setPostTag("</em>");
				}

				if (highlightRequestBuilder.getNumberOfFragments() == 0) {
					highlightRequestBuilder.setNumberOfFragments(1);
				}

				if (highlightRequestBuilder.getFragmentLength() == 0) {
					highlightRequestBuilder.setFragmentLength(100);
				}

			}

		}

		if (queryRequestBuilder.getAnalysisRequestList() != null) {

			for (AnalysisRequest analysisRequest : queryRequestBuilder.getAnalysisRequestList()) {

				AnalysisRequest.Builder analysisRequestBuilder = analysisRequest.toBuilder();

				if (analysisRequestBuilder.getTopN() == 0) {
					analysisRequestBuilder.setTopN(10);
				}

				if (analysisRequestBuilder.getTermSort() == null) {
					analysisRequestBuilder.setTermSort(AnalysisRequest.TermSort.TFIDF);
				}

				if (analysisRequestBuilder.getSummaryType() == null) {
					analysisRequestBuilder.setSummaryType(AnalysisRequest.SummaryType.ALL_TERMS_TOP_N);
				}

			}

		}

		return queryRequestBuilder.build();

	}
}
