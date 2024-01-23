package io.zulia.server.rest.controllers;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.annotation.QueryValue;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
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
public class TermsController {

	@ExecuteOn(TaskExecutors.BLOCKING)
	@Get(ZuliaRESTConstants.TERMS_URL)
	@Produces({ ZuliaRESTConstants.UTF8_JSON })
	public TermsResponseDTO getTermsJson(@QueryValue(ZuliaRESTConstants.INDEX) final String indexName,
			@QueryValue(ZuliaRESTConstants.FIELDS) final String field, @Nullable @QueryValue(ZuliaRESTConstants.AMOUNT) final Integer amount,
			@Nullable @QueryValue(ZuliaRESTConstants.MIN_DOC_FREQ) final Integer minDocFreq,
			@Nullable @QueryValue(ZuliaRESTConstants.MIN_TERM_FREQ) final Integer minTermFreq,
			@Nullable @QueryValue(ZuliaRESTConstants.START_TERM) final String startTerm,
			@Nullable @QueryValue(ZuliaRESTConstants.END_TERM) final String endTerm,
			@Nullable @QueryValue(ZuliaRESTConstants.TERM_FILTER) final String termFilter,
			@Nullable @QueryValue(ZuliaRESTConstants.TERM_MATCH) final String termMatch,
			@Nullable @QueryValue(ZuliaRESTConstants.INCLUDE_TERM) final List<String> includeTerm,
			@Nullable @QueryValue(ZuliaRESTConstants.FUZZY_TERM_JSON) final String fuzzyTermJson) throws Exception {

		ZuliaIndexManager indexManager = ZuliaNodeProvider.getZuliaNode().getIndexManager();

		GetTermsRequest termsRequest = getTermsRequest(indexName, field, amount, minDocFreq, minTermFreq, startTerm, endTerm, termFilter, termMatch,
				includeTerm, fuzzyTermJson);
		GetTermsResponse terms = indexManager.getTerms(termsRequest);

		TermsResponseDTO termsResponseDTO = new TermsResponseDTO();
		termsResponseDTO.setIndex(indexName);
		termsResponseDTO.setField(field);

		List<TermDTO> termsDTOs = terms.getTermList().stream().map(TermDTO::fromTerm).collect(Collectors.toList());
		termsResponseDTO.setTerms(termsDTOs);
		return termsResponseDTO;

	}

	@ExecuteOn(TaskExecutors.BLOCKING)
	@Get(ZuliaRESTConstants.TERMS_URL + "/csv")
	@Produces({ ZuliaRESTConstants.UTF8_CSV })
	public String getTermsCsv(@QueryValue(ZuliaRESTConstants.INDEX) final String indexName, @QueryValue(ZuliaRESTConstants.FIELDS) final String field,
			@Nullable @QueryValue(ZuliaRESTConstants.AMOUNT) final Integer amount,
			@Nullable @QueryValue(ZuliaRESTConstants.MIN_DOC_FREQ) final Integer minDocFreq,
			@Nullable @QueryValue(ZuliaRESTConstants.MIN_TERM_FREQ) final Integer minTermFreq,
			@Nullable @QueryValue(ZuliaRESTConstants.START_TERM) final String startTerm,
			@Nullable @QueryValue(ZuliaRESTConstants.END_TERM) final String endTerm,
			@Nullable @QueryValue(ZuliaRESTConstants.TERM_FILTER) final String termFilter,
			@Nullable @QueryValue(ZuliaRESTConstants.TERM_MATCH) final String termMatch,
			@Nullable @QueryValue(ZuliaRESTConstants.INCLUDE_TERM) final List<String> includeTerm,
			@Nullable @QueryValue(ZuliaRESTConstants.FUZZY_TERM_JSON) final String fuzzyTermJson) throws Exception {

		ZuliaIndexManager indexManager = ZuliaNodeProvider.getZuliaNode().getIndexManager();

		GetTermsRequest termsRequest = getTermsRequest(indexName, field, amount, minDocFreq, minTermFreq, startTerm, endTerm, termFilter, termMatch,
				includeTerm, fuzzyTermJson);
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
			String endTerm, String termFilter, String termMatch, List<String> includeTerm, String fuzzyTermJson) {
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
		return termsBuilder.build();
	}

}
