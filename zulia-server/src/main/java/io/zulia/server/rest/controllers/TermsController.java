package io.zulia.server.rest.controllers;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.annotation.QueryValue;
import io.micronaut.http.hateoas.JsonError;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.tags.Tag;
import io.zulia.ZuliaRESTConstants;
import io.zulia.rest.dto.TermDTO;
import io.zulia.rest.dto.TermsResponseDTO;
import io.zulia.server.index.ZuliaIndexManager;
import io.zulia.server.util.ZuliaNodeProvider;

import java.util.List;
import java.util.stream.Collectors;

import static io.zulia.message.ZuliaBase.FuzzyTerm;
import static io.zulia.message.ZuliaBase.Term;
import static io.zulia.message.ZuliaServiceOuterClass.GetTermsRequest;
import static io.zulia.message.ZuliaServiceOuterClass.GetTermsResponse;

/**
 * Created by Payam Meyer on 8/7/17.
 *
 * @author pmeyer
 */
@Controller
@Tag(name = "Terms")
@ApiResponses({ @ApiResponse(responseCode = "400", content = { @Content(schema = @Schema(implementation = JsonError.class)) }),
		@ApiResponse(responseCode = "500", content = { @Content(schema = @Schema(implementation = JsonError.class)) }),
		@ApiResponse(responseCode = "503", content = { @Content(schema = @Schema(implementation = JsonError.class)) }) })
@ExecuteOn(TaskExecutors.VIRTUAL)
public class TermsController {


	@Get(ZuliaRESTConstants.TERMS_URL)
	@Produces({ ZuliaRESTConstants.UTF8_JSON })
	@Operation(summary = "Get terms as JSON", description = "Returns terms and their frequencies for a given field in the index")
	public TermsResponseDTO getTermsJson(
			@Parameter(description = "Index name", required = true) @QueryValue(ZuliaRESTConstants.INDEX) final String indexName,
			@Parameter(description = "Field name to get terms for", required = true) @QueryValue(ZuliaRESTConstants.FIELDS) final String field,
			@Parameter(description = "Maximum number of terms to return") @Nullable @QueryValue(ZuliaRESTConstants.AMOUNT) final Integer amount,
			@Parameter(description = "Minimum document frequency for a term to be included") @Nullable @QueryValue(ZuliaRESTConstants.MIN_DOC_FREQ) final Integer minDocFreq,
			@Parameter(description = "Minimum term frequency for a term to be included") @Nullable @QueryValue(ZuliaRESTConstants.MIN_TERM_FREQ) final Integer minTermFreq,
			@Parameter(description = "Only return terms lexicographically >= this value") @Nullable @QueryValue(ZuliaRESTConstants.START_TERM) final String startTerm,
			@Parameter(description = "Only return terms lexicographically <= this value") @Nullable @QueryValue(ZuliaRESTConstants.END_TERM) final String endTerm,
			@Parameter(description = "Regex filter to apply to terms") @Nullable @QueryValue(ZuliaRESTConstants.TERM_FILTER) final String termFilter,
			@Parameter(description = "Term match pattern") @Nullable @QueryValue(ZuliaRESTConstants.TERM_MATCH) final String termMatch,
			@Parameter(description = "Specific terms to include in the response") @Nullable @QueryValue(ZuliaRESTConstants.INCLUDE_TERM) final List<String> includeTerm,
			@Parameter(description = "Fuzzy term configuration as JSON (protobuf FuzzyTerm format)") @Nullable @QueryValue(ZuliaRESTConstants.FUZZY_TERM_JSON) final String fuzzyTermJson,
			@Parameter(description = "If true, force a commit before reading to ensure latest terms are visible") @Nullable @QueryValue(ZuliaRESTConstants.REALTIME) final Boolean realtime) throws Exception {

		ZuliaIndexManager indexManager = ZuliaNodeProvider.getZuliaNode().getIndexManager();

		GetTermsRequest termsRequest = getTermsRequest(indexName, field, amount, minDocFreq, minTermFreq, startTerm, endTerm, termFilter, termMatch,
				includeTerm, fuzzyTermJson, realtime);
		GetTermsResponse terms = indexManager.getTerms(termsRequest);

		TermsResponseDTO termsResponseDTO = new TermsResponseDTO();
		termsResponseDTO.setIndex(indexName);
		termsResponseDTO.setField(field);

		List<TermDTO> termsDTOs = terms.getTermList().stream().map(TermDTO::fromTerm).collect(Collectors.toList());
		termsResponseDTO.setTerms(termsDTOs);
		return termsResponseDTO;

	}

	@Get(ZuliaRESTConstants.TERMS_URL + "/csv")
	@Produces({ ZuliaRESTConstants.UTF8_CSV })
	@Operation(summary = "Get terms as CSV", description = "Returns terms and their frequencies for a given field as CSV with columns: term, termFreq, docFreq, score")
	public String getTermsCsv(@QueryValue(ZuliaRESTConstants.INDEX) final String indexName, @QueryValue(ZuliaRESTConstants.FIELDS) final String field,
			@Nullable @QueryValue(ZuliaRESTConstants.AMOUNT) final Integer amount,
			@Nullable @QueryValue(ZuliaRESTConstants.MIN_DOC_FREQ) final Integer minDocFreq,
			@Nullable @QueryValue(ZuliaRESTConstants.MIN_TERM_FREQ) final Integer minTermFreq,
			@Nullable @QueryValue(ZuliaRESTConstants.START_TERM) final String startTerm,
			@Nullable @QueryValue(ZuliaRESTConstants.END_TERM) final String endTerm,
			@Nullable @QueryValue(ZuliaRESTConstants.TERM_FILTER) final String termFilter,
			@Nullable @QueryValue(ZuliaRESTConstants.TERM_MATCH) final String termMatch,
			@Nullable @QueryValue(ZuliaRESTConstants.INCLUDE_TERM) final List<String> includeTerm,
			@Nullable @QueryValue(ZuliaRESTConstants.FUZZY_TERM_JSON) final String fuzzyTermJson,
			@Nullable @QueryValue(ZuliaRESTConstants.REALTIME) final Boolean realtime) throws Exception {

		ZuliaIndexManager indexManager = ZuliaNodeProvider.getZuliaNode().getIndexManager();

		GetTermsRequest termsRequest = getTermsRequest(indexName, field, amount, minDocFreq, minTermFreq, startTerm, endTerm, termFilter, termMatch,
				includeTerm, fuzzyTermJson,realtime);
		GetTermsResponse terms = indexManager.getTerms(termsRequest);

		StringBuilder csvString = new StringBuilder();

		csvString.append("term");
		csvString.append(",");
		csvString.append("termFreq");
		csvString.append(",");
		csvString.append("docFreq");
		if (termsRequest.hasFuzzyTerm()) {
			csvString.append(",");
			csvString.append("score");
		}
		csvString.append("\n");

		for (Term term : terms.getTermList()) {
			csvString.append(CSVUtil.quoteForCSV(term.getValue()));
			csvString.append(",");
			csvString.append(term.getTermFreq());
			csvString.append(",");
			csvString.append(term.getDocFreq());
			csvString.append(",");
			csvString.append(term.getScore());
			csvString.append("\n");
		}

		return csvString.toString();

	}

	private static GetTermsRequest getTermsRequest(String indexName, String field, Integer amount, Integer minDocFreq, Integer minTermFreq, String startTerm,
			String endTerm, String termFilter, String termMatch, List<String> includeTerm, String fuzzyTermJson, Boolean realtime) {
		GetTermsRequest.Builder termsBuilder = GetTermsRequest.newBuilder();
		termsBuilder.setIndexName(indexName);
		termsBuilder.setFieldName(field);
		if (amount != null) {
			termsBuilder.setAmount(amount);
		}

		if (minDocFreq != null) {
			termsBuilder.setMinDocFreq(minDocFreq);
		}
		if (minTermFreq != null) {
			termsBuilder.setMinTermFreq(minTermFreq);
		}

		if (startTerm != null) {
			termsBuilder.setStartTerm(startTerm);
		}
		if (endTerm != null) {
			termsBuilder.setEndTerm(endTerm);
		}

		if (termFilter != null) {
			termsBuilder.setTermFilter(termFilter);
		}
		if (termMatch != null) {
			termsBuilder.setTermMatch(termMatch);
		}

		if (includeTerm != null) {
			termsBuilder.addAllIncludeTerm(includeTerm);
		}

		if (fuzzyTermJson != null) {
			try {
				FuzzyTerm.Builder fuzzyTermBuilder = FuzzyTerm.newBuilder();
				JsonFormat.parser().merge(fuzzyTermJson, fuzzyTermBuilder);
				termsBuilder.setFuzzyTerm(fuzzyTermBuilder);
			}
			catch (InvalidProtocolBufferException e) {
				throw new IllegalArgumentException("Failed to parse fuzzy term json: " + e.getMessage());
			}

		}

		if (realtime != null) {
			termsBuilder.setRealtime(realtime);
		}
		return termsBuilder.build();
	}

}
