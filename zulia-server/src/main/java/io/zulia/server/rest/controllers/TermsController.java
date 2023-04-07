package io.zulia.server.rest.controllers;

import com.cedarsoftware.util.io.JsonWriter;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.annotation.QueryValue;
import io.zulia.ZuliaRESTConstants;
import io.zulia.server.index.ZuliaIndexManager;
import io.zulia.server.util.ZuliaNodeProvider;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;

import static io.zulia.message.ZuliaBase.FuzzyTerm;
import static io.zulia.message.ZuliaBase.Term;
import static io.zulia.message.ZuliaServiceOuterClass.GetTermsRequest;
import static io.zulia.message.ZuliaServiceOuterClass.GetTermsResponse;

/**
 * Created by Payam Meyer on 8/7/17.
 *
 * @author pmeyer
 */
@Controller(ZuliaRESTConstants.TERMS_URL)
public class TermsController {

	@Get
	@Produces({ MediaType.APPLICATION_JSON + ";charset=utf-8", MediaType.TEXT_PLAIN + ";charset=utf-8" })
	public HttpResponse<?> get(@QueryValue(ZuliaRESTConstants.INDEX) final String indexName, @QueryValue(ZuliaRESTConstants.FIELDS) final String field,
			@Nullable @QueryValue(ZuliaRESTConstants.AMOUNT) final Integer amount,
			@Nullable @QueryValue(ZuliaRESTConstants.MIN_DOC_FREQ) final Integer minDocFreq,
			@Nullable @QueryValue(ZuliaRESTConstants.MIN_TERM_FREQ) final Integer minTermFreq,
			@Nullable @QueryValue(ZuliaRESTConstants.START_TERM) final String startTerm,
			@Nullable @QueryValue(ZuliaRESTConstants.END_TERM) final String endTerm,
			@Nullable @QueryValue(ZuliaRESTConstants.TERM_FILTER) final String termFilter,
			@Nullable @QueryValue(ZuliaRESTConstants.TERM_MATCH) final String termMatch,
			@Nullable @QueryValue(ZuliaRESTConstants.INCLUDE_TERM) final List<String> includeTerm,
			@Nullable @QueryValue(ZuliaRESTConstants.FUZZY_TERM_JSON) final String fuzzyTermJson,
			@QueryValue(value = ZuliaRESTConstants.PRETTY, defaultValue = "true") Boolean pretty,
			@QueryValue(value = ZuliaRESTConstants.FORMAT, defaultValue = "json") final String format) {

		ZuliaIndexManager indexManager = ZuliaNodeProvider.getZuliaNode().getIndexManager();

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
				return HttpResponse.ok("Failed to parse analyzer json: " + e.getClass().getSimpleName() + ":" + e.getMessage())
						.status(ZuliaRESTConstants.INTERNAL_ERROR);
			}

		}

		try {
			GetTermsResponse terms = indexManager.getTerms(termsBuilder.build());

			if (format.equalsIgnoreCase("json")) {
				Document document = new Document();
				document.put("index", indexName);
				document.put("field", field);

				List<Document> termsDocs = new ArrayList<>();
				for (Term term : terms.getTermList()) {
					Document termDoc = new Document();
					termDoc.put("term", term.getValue());
					termDoc.put("docFreq", term.getDocFreq());
					termDoc.put("termFreq", term.getTermFreq());
					termDoc.put("score", term.getScore());
					termsDocs.add(termDoc);
				}

				document.put("terms", termsDocs);
				String docString = document.toJson();

				if (pretty) {
					docString = JsonWriter.formatJson(docString);
				}

				return HttpResponse.ok(docString).status(ZuliaRESTConstants.SUCCESS).contentType(MediaType.APPLICATION_JSON_TYPE + ";charset=utf-8");
			}
			else {

				StringBuilder csvString = new StringBuilder();

				csvString.append("term");
				csvString.append(",");
				csvString.append("termFreq");
				csvString.append(",");
				csvString.append("docFreq");
				if (termsBuilder.hasFuzzyTerm()) {
					csvString.append(",");
					csvString.append("score");
				}
				csvString.append("\n");

				for (Term term : terms.getTermList()) {
					String value = term.getValue();
					if (value.contains(",") || value.contains(" ") || value.contains("\"") || value.contains("\n")) {
						csvString.append("\"");
						csvString.append(value.replace("\"", "\"\""));
						csvString.append("\"");
					}
					else {
						csvString.append(value);
					}
					csvString.append(",");
					csvString.append(term.getTermFreq());
					csvString.append(",");
					csvString.append(term.getDocFreq());
					csvString.append(",");
					csvString.append(term.getScore());
					csvString.append("\n");
				}
				return HttpResponse.ok(csvString).status(ZuliaRESTConstants.SUCCESS).contentType(MediaType.TEXT_PLAIN + ";charset=utf-8");
			}

		}
		catch (Exception e) {
			return HttpResponse.serverError("Failed to fetch fields for index <" + indexName + ">: " + e.getMessage())
					.status(ZuliaRESTConstants.INTERNAL_ERROR);
		}

	}

}
