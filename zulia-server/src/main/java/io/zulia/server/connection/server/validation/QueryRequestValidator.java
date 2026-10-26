package io.zulia.server.connection.server.validation;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import io.zulia.ZuliaConstants;
import io.zulia.message.ZuliaQuery;
import io.zulia.message.ZuliaQuery.AnalysisRequest;
import io.zulia.message.ZuliaQuery.DistinctCountRequest;
import io.zulia.message.ZuliaQuery.HighlightRequest;
import io.zulia.message.ZuliaServiceOuterClass.QueryRequest;
import io.zulia.server.search.distinct.DistinctCountCollectorManager;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.zulia.message.ZuliaQuery.CountRequest;
import static io.zulia.message.ZuliaQuery.FacetRequest;

/**
 * Created by Payam Meyer on 9/19/17.
 *
 * @author pmeyer
 */
public class QueryRequestValidator implements DefaultValidator<QueryRequest> {

	/**
	 * A stored warming search gets the same defaults and checks as a live query, plus the label that updates address it by.
	 */
	public static QueryRequest validateWarmingSearch(ByteString bytes) {
		QueryRequest queryRequest;
		try {
			queryRequest = QueryRequest.parseFrom(bytes);
		}
		catch (InvalidProtocolBufferException e) {
			throw new IllegalArgumentException("Failed to parse QueryRequest from warming search bytes", e);
		}
		if (queryRequest.getSearchLabel().isEmpty()) {
			throw new IllegalArgumentException("A search label is required for a warming search");
		}
		return new QueryRequestValidator().validateAndSetDefault(queryRequest);
	}

	@Override
	public QueryRequest validateAndSetDefault(QueryRequest request) {
		QueryRequest.Builder queryRequestBuilder = request.toBuilder();

		for (ZuliaQuery.Query.Builder queryBuilder : queryRequestBuilder.getQueryBuilderList()) {
			if (queryBuilder.getBoost() < 0f) {
				throw new IllegalArgumentException("Query boost must be >= 0, got " + queryBuilder.getBoost());
			}
			if (queryBuilder.getQueryType() == ZuliaQuery.Query.QueryType.MORE_LIKE_THIS) {
				ZuliaQuery.MoreLikeThisParams mltParams = queryBuilder.getMoreLikeThisParams();
				if (mltParams.getVectorTopN() < 0) {
					throw new IllegalArgumentException("More-like-this vectorTopN must be >= 0, got " + mltParams.getVectorTopN());
				}
				if (mltParams.getMaxDocFreqPct() > 100) {
					throw new IllegalArgumentException("More-like-this maxDocFreqPct must be in [0, 100], got " + mltParams.getMaxDocFreqPct());
				}
				if (!mltParams.getVectorField().isEmpty() && mltParams.getVectorTopN() == 0) {
					queryBuilder.getMoreLikeThisParamsBuilder().setVectorTopN(100);
				}
			}
		}

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

		Set<String> distinctCountFields = new HashSet<>();
		long distinctCountKeysPerShard = 0;
		for (DistinctCountRequest.Builder distinctCountBuilder : queryRequestBuilder.getDistinctCountBuilderList()) {
			String field = distinctCountBuilder.getField();
			if (field.isEmpty()) {
				throw new IllegalArgumentException("Distinct count requires a field");
			}
			if (!distinctCountFields.add(field)) {
				throw new IllegalArgumentException("Distinct count field <" + field + "> is requested more than once");
			}
			// the proto field is uint32, so a value at or above 2^31 reads back negative here
			long maxGroupsPerShard = Integer.toUnsignedLong(distinctCountBuilder.getMaxGroupsPerShard());
			if (maxGroupsPerShard > DistinctCountCollectorManager.MAX_MAX_GROUPS_PER_SHARD) {
				throw new IllegalArgumentException("Distinct count field <" + field + "> has maxGroupsPerShard " + maxGroupsPerShard
						+ " (a negative number reads back this way). Each distinct value costs 8 bytes per shard on the wire, so the limit is "
						+ DistinctCountCollectorManager.MAX_MAX_GROUPS_PER_SHARD + " (128 MB per field per shard). Use 0 for the default of "
						+ DistinctCountCollectorManager.DEFAULT_MAX_GROUPS_PER_SHARD);
			}
			if (maxGroupsPerShard == 0) {
				distinctCountBuilder.setMaxGroupsPerShard(DistinctCountCollectorManager.DEFAULT_MAX_GROUPS_PER_SHARD);
			}
			distinctCountKeysPerShard += distinctCountBuilder.getMaxGroupsPerShard();
		}
		// one node answers for every shard it holds in one message, so the per shard total across fields is bounded like a single field
		if (distinctCountKeysPerShard > DistinctCountCollectorManager.MAX_MAX_GROUPS_PER_SHARD) {
			throw new IllegalArgumentException(
					"Distinct count caps across all fields total " + distinctCountKeysPerShard + " keys per shard, more than the limit of "
							+ DistinctCountCollectorManager.MAX_MAX_GROUPS_PER_SHARD
							+ ". Every shard a node holds ships its keys in one message, so lower the caps or count fewer fields at once");
		}

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
