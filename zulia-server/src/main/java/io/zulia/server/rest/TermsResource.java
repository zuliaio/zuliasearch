package io.zulia.server.rest;

import com.cedarsoftware.util.io.JsonWriter;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.zulia.ZuliaConstants;
import io.zulia.server.index.ZuliaIndexManager;
import org.bson.Document;

import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
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
@Path(ZuliaConstants.TERMS_URL)
public class TermsResource {

	private ZuliaIndexManager indexManager;

	public TermsResource(ZuliaIndexManager indexManager) {
		this.indexManager = indexManager;
	}

	@GET
	@Produces({ MediaType.APPLICATION_JSON + ";charset=utf-8", MediaType.TEXT_PLAIN + ";charset=utf-8" })
	public Response get(@Context Response response, @QueryParam(ZuliaConstants.INDEX) final String indexName,
			@QueryParam(ZuliaConstants.FIELDS) final String field, @QueryParam(ZuliaConstants.AMOUNT) final Integer amount,
			@QueryParam(ZuliaConstants.MIN_DOC_FREQ) final Integer minDocFreq, @QueryParam(ZuliaConstants.MIN_TERM_FREQ) final Integer minTermFreq,
			@QueryParam(ZuliaConstants.START_TERM) final String startTerm, @QueryParam(ZuliaConstants.END_TERM) final String endTerm,
			@QueryParam(ZuliaConstants.TERM_FILTER) final String termFilter, @QueryParam(ZuliaConstants.TERM_MATCH) final String termMatch,
			@QueryParam(ZuliaConstants.INCLUDE_TERM) final List<String> includeTerm, @QueryParam(ZuliaConstants.FUZZY_TERM_JSON) final String fuzzyTermJson,
			@QueryParam(ZuliaConstants.PRETTY) boolean pretty, @QueryParam(ZuliaConstants.FORMAT) @DefaultValue("json") final String format) {

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
					return Response.status(ZuliaConstants.INTERNAL_ERROR)
							.entity("Failed to parse analyzer json: " + e.getClass().getSimpleName() + ":" + e.getMessage()).build();
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

					return Response.status(ZuliaConstants.SUCCESS).type(MediaType.APPLICATION_JSON_TYPE + ";charset=utf-8").entity(docString).build();
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
					return Response.status(ZuliaConstants.SUCCESS).type(MediaType.TEXT_PLAIN + ";charset=utf-8").entity(csvString.toString()).build();
				}

			}
			catch (Exception e) {
				return Response.status(ZuliaConstants.INTERNAL_ERROR).entity("Failed to fetch fields for index <" + indexName + ">: " + e.getMessage()).build();
			}
		}
		else {
			return Response.status(ZuliaConstants.INTERNAL_ERROR).entity("No index or field defined").build();
		}

	}

}
