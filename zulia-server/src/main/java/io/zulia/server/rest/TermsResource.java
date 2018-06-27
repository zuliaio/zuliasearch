package io.zulia.server.rest;

import com.cedarsoftware.util.io.JsonWriter;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.micronaut.context.annotation.Parameter;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.annotation.QueryValue;
import io.zulia.ZuliaConstants;
import io.zulia.server.index.ZuliaIndexManager;
import org.bson.Document;

import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.List;

import static io.zulia.message.ZuliaBase.FuzzyTerm;
import static io.zulia.message.ZuliaBase.Term;
import static io.zulia.message.ZuliaServiceOuterClass.GetTermsRequest;
import static io.zulia.message.ZuliaServiceOuterClass.GetTermsResponse;

/**
 * Created by Payam Meyer on 8/7/17.
 * @author pmeyer
 */
@Controller(ZuliaConstants.TERMS_URL)
public class TermsResource {

	@Singleton
	private ZuliaIndexManager indexManager;

	public TermsResource(ZuliaIndexManager indexManager) {
		this.indexManager = indexManager;
	}

	@Get
	@Produces({ MediaType.APPLICATION_JSON + ";charset=utf-8", MediaType.TEXT_PLAIN + ";charset=utf-8" })
	public HttpResponse get(@Parameter(ZuliaConstants.INDEX) final String indexName, @Parameter(ZuliaConstants.FIELDS) final String field,
			@Parameter(ZuliaConstants.AMOUNT) final Integer amount, @Parameter(ZuliaConstants.MIN_DOC_FREQ) final Integer minDocFreq,
			@Parameter(ZuliaConstants.MIN_TERM_FREQ) final Integer minTermFreq, @Parameter(ZuliaConstants.START_TERM) final String startTerm,
			@Parameter(ZuliaConstants.END_TERM) final String endTerm, @Parameter(ZuliaConstants.TERM_FILTER) final String termFilter,
			@Parameter(ZuliaConstants.TERM_MATCH) final String termMatch, @Parameter(ZuliaConstants.INCLUDE_TERM) final List<String> includeTerm,
			@Parameter(ZuliaConstants.FUZZY_TERM_JSON) final String fuzzyTermJson, @Parameter(ZuliaConstants.PRETTY) boolean pretty,
			@Parameter(ZuliaConstants.FORMAT) @QueryValue("json") final String format) {

		if (indexName != null && field != null) {

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
					return HttpResponse.created("Failed to parse analyzer json: " + e.getClass().getSimpleName() + ":" + e.getMessage())
							.status(ZuliaConstants.INTERNAL_ERROR);
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

					return HttpResponse.created(docString).status(ZuliaConstants.SUCCESS).contentType(MediaType.APPLICATION_JSON_TYPE + ";charset=utf-8");
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
					return HttpResponse.created(csvString).status(ZuliaConstants.SUCCESS).contentType(MediaType.TEXT_PLAIN + ";charset=utf-8");
				}

			}
			catch (Exception e) {
				return HttpResponse.created("Failed to fetch fields for index <" + indexName + ">: " + e.getMessage()).status(ZuliaConstants.INTERNAL_ERROR);
			}
		}
		else {
			return HttpResponse.created("No index or field defined").status(ZuliaConstants.INTERNAL_ERROR);
		}

	}

}
