package io.zulia.server.connection.server.validation;

import io.zulia.ZuliaConstants;
import io.zulia.message.ZuliaQuery;
import io.zulia.message.ZuliaQuery.AnalysisRequest;
import io.zulia.message.ZuliaQuery.HighlightRequest;
import io.zulia.message.ZuliaServiceOuterClass.QueryRequest;

import java.util.HashMap;
import java.util.Map;

import static io.zulia.message.ZuliaQuery.CountRequest;
import static io.zulia.message.ZuliaQuery.FacetRequest;

/**
 * Created by Payam Meyer on 9/19/17.
 *
 * @author pmeyer
 */
public class QueryRequestValidator implements DefaultValidator<QueryRequest> {

	@Override
	public QueryRequest validateAndSetDefault(QueryRequest request) {
		QueryRequest.Builder queryRequestBuilder = request.toBuilder();

		ZuliaQuery.FetchType resultFetchType = queryRequestBuilder.getResultFetchType();
		boolean fetchDocument = ZuliaQuery.FetchType.FULL.equals(resultFetchType) || ZuliaQuery.FetchType.ALL.equals(resultFetchType);

		FacetRequest.Builder facetRequestBuilder = queryRequestBuilder.getFacetRequestBuilder();

		for (ZuliaQuery.StatRequest.Builder statRequestBuilder : facetRequestBuilder.getStatRequestBuilderList()) {
			if (statRequestBuilder.getMaxFacets() == 0) {
				statRequestBuilder.setMaxFacets(10);
			}
			if (statRequestBuilder.getShardFacets() == 0) {
				statRequestBuilder.setShardFacets(statRequestBuilder.getMaxFacets() * 10);
			}
			// Cannot compute percentiles if precision is 0
			if (statRequestBuilder.getPercentilesCount() > 0 && statRequestBuilder.getPrecision() == 0.0) {
				statRequestBuilder.setPrecision(0.001);
			}
			if (statRequestBuilder.getPrecision() < 0.0) {
				throw new IllegalArgumentException("Percentile precision must be a number > 0.0");
			}
			if (statRequestBuilder.getPercentilesList().stream().anyMatch(value -> (0.0 > value || value > 1.0))) {
				throw new IllegalArgumentException("Percentiles must be in the range [0.0, 1.0]");
			}
		}

		for (CountRequest.Builder countRequestBuilder : facetRequestBuilder.getCountRequestBuilderList()) {

			if (countRequestBuilder.getMaxFacets() == 0) {
				countRequestBuilder.setMaxFacets(10);
			}
			if (countRequestBuilder.getShardFacets() == 0) {
				countRequestBuilder.setShardFacets(countRequestBuilder.getMaxFacets() * 10);
			}
		}

		Map<String, CountRequest.Builder> uniqueRequests = new HashMap<>();
		for (CountRequest.Builder builder : facetRequestBuilder.getCountRequestBuilderList()) {
			ZuliaQuery.Facet facetField = builder.getFacetField();
			StringBuilder facet = new StringBuilder(facetField.getLabel());
			if (facetField.getPathCount() > 0) {
				for (String pathPiece : facetField.getPathList()) {
					facet.append(ZuliaConstants.FACET_PATH_DELIMITER).append(pathPiece);
				}
			}
			String facetString = facet.toString();
			CountRequest.Builder otherRequest = uniqueRequests.get(facetString);
			if (otherRequest != null) {
				otherRequest.setShardFacets(Math.max(builder.getShardFacets(), otherRequest.getShardFacets()));
				otherRequest.setMaxFacets(Math.max(builder.getMaxFacets(), otherRequest.getMaxFacets()));
			}
			else {
				uniqueRequests.put(facetString, builder);
			}
		}
		facetRequestBuilder.clearCountRequest();
		facetRequestBuilder.addAllCountRequest(uniqueRequests.values().stream().map(CountRequest.Builder::build).toList());

		if (queryRequestBuilder.getHighlightRequestList() != null) {

			for (HighlightRequest.Builder highlightRequestBuilder : queryRequestBuilder.getHighlightRequestBuilderList()) {

				if (highlightRequestBuilder.getPreTag().isEmpty()) {
					highlightRequestBuilder.setPreTag("<em>");
				}

				if (highlightRequestBuilder.getPostTag().isEmpty()) {
					highlightRequestBuilder.setPostTag("</em>");
				}

				if (highlightRequestBuilder.getNumberOfFragments() == 0) {
					highlightRequestBuilder.setNumberOfFragments(1);
				}

				if (highlightRequestBuilder.getFragmentLength() == 0) {
					highlightRequestBuilder.setFragmentLength(100);
				}

			}

			if (!queryRequestBuilder.getHighlightRequestBuilderList().isEmpty() && !fetchDocument) {
				throw new IllegalArgumentException("Highlighting requires a full fetch of the document");
			}

		}

		if (queryRequestBuilder.getAnalysisRequestList() != null) {

			for (AnalysisRequest.Builder analysisRequestBuilder : queryRequestBuilder.getAnalysisRequestBuilderList()) {

				if (analysisRequestBuilder.getTopN() == 0) {
					analysisRequestBuilder.setTopN(10);
				}

			}

			if (!queryRequestBuilder.getAnalysisRequestBuilderList().isEmpty() && !fetchDocument) {
				throw new IllegalArgumentException("Analysis requires a full fetch of the document");
			}

		}

		return queryRequestBuilder.build();

	}
}
