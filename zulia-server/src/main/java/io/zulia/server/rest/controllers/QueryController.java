package io.zulia.server.rest.controllers;

import com.cedarsoftware.util.io.JsonWriter;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.micronaut.context.annotation.Parameter;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.io.Writable;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.annotation.QueryValue;
import io.zulia.ZuliaConstants;
import io.zulia.server.index.ZuliaIndexManager;
import io.zulia.server.util.ZuliaNodeProvider;
import io.zulia.util.CursorHelper;
import io.zulia.util.ResultHelper;
import org.bson.Document;
import org.bson.json.JsonWriterSettings;

import java.io.Writer;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static io.zulia.message.ZuliaBase.Similarity;
import static io.zulia.message.ZuliaBase.TermOrBuilder;
import static io.zulia.message.ZuliaQuery.*;
import static io.zulia.message.ZuliaServiceOuterClass.QueryRequest;
import static io.zulia.message.ZuliaServiceOuterClass.QueryResponse;

/**
 * Created by Payam Meyer on 8/7/17.
 * @author pmeyer
 */
@Controller(ZuliaConstants.QUERY_URL)
public class QueryController {

	private final static Logger LOG = Logger.getLogger(QueryController.class.getSimpleName());

	@Get
	@Produces({ MediaType.APPLICATION_JSON + ";charset=utf-8", MediaType.TEXT_PLAIN + ";charset=utf-8" })
	public HttpResponse<?> get(@QueryValue(ZuliaConstants.INDEX) List<String> indexName,
			@QueryValue(value = ZuliaConstants.QUERY, defaultValue = "*:*") String query,
			@Nullable @QueryValue(ZuliaConstants.QUERY_FIELD) List<String> queryFields,
			@Nullable @QueryValue(ZuliaConstants.FILTER_QUERY) List<String> filterQueries,
			@Nullable @QueryValue(ZuliaConstants.QUERY_JSON) List<String> queryJsonList, @Nullable @QueryValue(ZuliaConstants.FIELDS) List<String> fields,
			@QueryValue(value = ZuliaConstants.FETCH, defaultValue = "true") Boolean fetch,
			@QueryValue(value = ZuliaConstants.ROWS, defaultValue = "0") Integer rows, @Nullable @QueryValue(ZuliaConstants.FACET) List<String> facet,
			@Nullable @QueryValue(ZuliaConstants.DRILL_DOWN) List<String> drillDowns, @Nullable @QueryValue(ZuliaConstants.DEFAULT_OP) String defaultOperator,
			@Nullable @QueryValue(ZuliaConstants.SORT) List<String> sort, @QueryValue(value = ZuliaConstants.PRETTY, defaultValue = "true") Boolean pretty,
			@Nullable @QueryValue(value = ZuliaConstants.DISMAX, defaultValue = "false") Boolean dismax,
			@Nullable @QueryValue(ZuliaConstants.DISMAX_TIE) Float dismaxTie, @Nullable @QueryValue(ZuliaConstants.MIN_MATCH) Integer mm,
			@Nullable @QueryValue(ZuliaConstants.SIMILARITY) List<String> similarity,
			@QueryValue(value = ZuliaConstants.DEBUG, defaultValue = "false") Boolean debug,
			@QueryValue(value = ZuliaConstants.DONT_CACHE, defaultValue = "true") Boolean dontCache, @Nullable @QueryValue(ZuliaConstants.START) Integer start,
			@Nullable @QueryValue(ZuliaConstants.HIGHLIGHT) List<String> highlightList,
			@Nullable @QueryValue(ZuliaConstants.HIGHLIGHT_JSON) List<String> highlightJsonList,
			@Nullable @QueryValue(ZuliaConstants.ANALYZE_JSON) List<String> analyzeJsonList,
			@QueryValue(value = ZuliaConstants.FORMAT, defaultValue = "json") String format,
			@QueryValue(value = ZuliaConstants.BATCH, defaultValue = "false") Boolean batch,
			@QueryValue(value = ZuliaConstants.BATCH_SIZE, defaultValue = "500") Integer batchSize, @Nullable @QueryValue(ZuliaConstants.CURSOR) String cursor,
			@QueryValue(value = ZuliaConstants.TRUNCATE, defaultValue = "false") Boolean truncate) {

		ZuliaIndexManager indexManager = ZuliaNodeProvider.getZuliaNode().getIndexManager();

		QueryRequest.Builder qrBuilder = QueryRequest.newBuilder().addAllIndex(indexName);

		try {
			boolean outputCursor = false;
			if (cursor != null) {
				if (!cursor.equals("0")) {
					qrBuilder.setLastResult(CursorHelper.getLastResultFromCursor(cursor));
				}
				outputCursor = true;
				if (sort == null || sort.isEmpty()) {
					return HttpResponse.created("Sort on unique value or value combination is required to use a cursor (i.e. id or title,id)")
							.status(ZuliaConstants.INTERNAL_ERROR);
				}
			}

			if (debug != null) {
				qrBuilder.setDebug(debug);
			}

			if (start != null) {
				qrBuilder.setStart(start);
			}

			if (dontCache != null) {
				qrBuilder.setDontCache(dontCache);
			}

			Query.Builder mainQueryBuilder = Query.newBuilder();
			if (query != null) {
				mainQueryBuilder.setQ(query);
			}
			if (mm != null) {
				mainQueryBuilder.setMm(mm);
			}
			if (dismax != null) {
				mainQueryBuilder.setDismax(dismax);
				if (dismaxTie != null) {
					mainQueryBuilder.setDismaxTie(dismaxTie);
				}
			}
			if (queryFields != null) {
				mainQueryBuilder.addAllQf(queryFields);
			}
			if (defaultOperator != null) {
				if (defaultOperator.equalsIgnoreCase("AND")) {
					mainQueryBuilder.setDefaultOp(Query.Operator.AND);
				}
				else if (defaultOperator.equalsIgnoreCase("OR")) {
					mainQueryBuilder.setDefaultOp(Query.Operator.OR);
				}
				else {
					HttpResponse.created("Invalid default operator <" + defaultOperator + ">").status(ZuliaConstants.INTERNAL_ERROR);
				}
			}
			mainQueryBuilder.setQueryType(Query.QueryType.SCORE_MUST);

			qrBuilder.addQuery(mainQueryBuilder);

			if (similarity != null) {
				for (String sim : similarity) {
					if (sim.contains(":")) {
						int i = sim.indexOf(":");
						String field = sim.substring(0, i);
						String simType = sim.substring(i + 1);

						FieldSimilarity.Builder fieldSimilarity = FieldSimilarity.newBuilder();
						fieldSimilarity.setField(field);

						if (simType.equalsIgnoreCase("bm25")) {
							fieldSimilarity.setSimilarity(Similarity.BM25);
						}
						else if (simType.equalsIgnoreCase("constant")) {
							fieldSimilarity.setSimilarity(Similarity.CONSTANT);
						}
						else if (simType.equalsIgnoreCase("tf")) {
							fieldSimilarity.setSimilarity(Similarity.TF);
						}
						else if (simType.equalsIgnoreCase("tfidf")) {
							fieldSimilarity.setSimilarity(Similarity.TFIDF);
						}
						else {
							HttpResponse.created("Unknown similarity type <" + simType + ">").status(ZuliaConstants.INTERNAL_ERROR);
						}

						qrBuilder.addFieldSimilarity(fieldSimilarity);
					}
					else {
						HttpResponse.created("Similarity <" + sim + "> should be in the form field:simType").status(ZuliaConstants.INTERNAL_ERROR);
					}
				}
			}

			if (filterQueries != null) {
				for (String filterQuery : filterQueries) {
					Query filterQueryBuilder = Query.newBuilder().setQ(filterQuery).setQueryType(Query.QueryType.FILTER).build();
					qrBuilder.addQuery(filterQueryBuilder);
				}
			}

			if (queryJsonList != null) {
				for (String queryJson : queryJsonList) {
					try {
						Query.Builder subQueryBuilder = Query.newBuilder();
						JsonFormat.parser().merge(queryJson, subQueryBuilder);
						qrBuilder.addQuery(subQueryBuilder);
					}
					catch (InvalidProtocolBufferException e) {
						return HttpResponse.created("Failed to parse query json: " + e.getClass().getSimpleName() + ":" + e.getMessage())
								.status(ZuliaConstants.INTERNAL_ERROR);
					}
				}
			}

			if (highlightList != null) {
				for (String hl : highlightList) {
					HighlightRequest highlightRequest = HighlightRequest.newBuilder().setField(hl).build();
					qrBuilder.addHighlightRequest(highlightRequest);
				}
			}

			if (highlightJsonList != null) {
				for (String hlJson : highlightJsonList) {
					try {
						HighlightRequest.Builder hlBuilder = HighlightRequest.newBuilder();
						JsonFormat.parser().merge(hlJson, hlBuilder);
						qrBuilder.addHighlightRequest(hlBuilder);
					}
					catch (InvalidProtocolBufferException e) {
						return HttpResponse.created("Failed to parse highlight json: " + e.getClass().getSimpleName() + ":" + e.getMessage())
								.status(ZuliaConstants.INTERNAL_ERROR);
					}
				}
			}

			if (analyzeJsonList != null) {
				for (String alJson : analyzeJsonList) {
					try {
						AnalysisRequest.Builder analyzeRequestBuilder = AnalysisRequest.newBuilder();
						JsonFormat.parser().merge(alJson, analyzeRequestBuilder);
						qrBuilder.addAnalysisRequest(analyzeRequestBuilder);
					}
					catch (InvalidProtocolBufferException e) {
						return HttpResponse.created("Failed to parse analyzer json: " + e.getClass().getSimpleName() + ":" + e.getMessage())
								.status(ZuliaConstants.INTERNAL_ERROR);
					}
				}
			}

			if (fields != null) {
				for (String field : fields) {
					if (field.startsWith("-")) {
						qrBuilder.addDocumentMaskedFields(field.substring(1));
					}
					else {
						qrBuilder.addDocumentFields(field);
					}
				}
			}

			qrBuilder.setResultFetchType(FetchType.FULL);
			if (fetch != null && !fetch) {
				qrBuilder.setResultFetchType(FetchType.NONE);
			}

			FacetRequest.Builder frBuilder = FacetRequest.newBuilder();
			if (facet != null) {
				for (String f : facet) {
					Integer count = null;
					if (f.contains(":")) {
						String countString = f.substring(f.indexOf(":") + 1);
						f = f.substring(0, f.indexOf(":"));
						try {
							count = Integer.parseInt(countString);
						}
						catch (Exception e) {
							return HttpResponse.created("Invalid facet count <" + countString + "> for facet <" + f + ">")
									.status(ZuliaConstants.INTERNAL_ERROR);
						}
					}

					CountRequest.Builder countBuilder = CountRequest.newBuilder();
					Facet zuliaFacet = Facet.newBuilder().setLabel(f).build();
					CountRequest.Builder facetBuilder = countBuilder.setFacetField(zuliaFacet);
					if (count != null) {
						facetBuilder.setMaxFacets(count);
					}

					frBuilder.addCountRequest(facetBuilder);
				}
			}

			if (drillDowns != null) {
				for (String drillDown : drillDowns) {
					if (drillDown.contains(":")) {
						String value = drillDown.substring(drillDown.indexOf(":") + 1);
						String field = drillDown.substring(0, drillDown.indexOf(":"));
						frBuilder.addDrillDown(Facet.newBuilder().setLabel(field).setValue(value));
					}
				}
			}

			qrBuilder.setFacetRequest(frBuilder);

			if (sort != null) {
				SortRequest.Builder sortRequest = SortRequest.newBuilder();
				for (String sortField : sort) {

					FieldSort.Builder fieldSort = FieldSort.newBuilder();
					if (sortField.contains(":")) {
						String sortDir = sortField.substring(sortField.indexOf(":") + 1);
						sortField = sortField.substring(0, sortField.indexOf(":"));

						if ("-1".equals(sortDir) || "DESC".equalsIgnoreCase(sortDir)) {
							fieldSort.setDirection(FieldSort.Direction.DESCENDING);
						}
						else if ("1".equals(sortDir) || "ASC".equalsIgnoreCase(sortDir)) {
							fieldSort.setDirection(FieldSort.Direction.ASCENDING);
						}
						else {
							return HttpResponse.created("Invalid sort direction <" + sortDir + "> for field <" + sortField + ">.  Expecting -1/1 or DESC/ASC")
									.status(ZuliaConstants.INTERNAL_ERROR);
						}
					}
					fieldSort.setSortField(sortField);
					sortRequest.addFieldSort(fieldSort);
				}
				qrBuilder.setSortRequest(sortRequest);
			}
			qrBuilder.setAmount(rows);

			if (format.equals("json")) {
				QueryResponse qr = indexManager.query(qrBuilder.build());
				String response = getStandardResponse(qr, !pretty, outputCursor, truncate);

				if (pretty) {
					response = JsonWriter.formatJson(response);
				}

				return HttpResponse.ok(response).status(ZuliaConstants.SUCCESS).contentType(MediaType.APPLICATION_JSON_TYPE);
			}
			else {
				if (fields != null && !fields.isEmpty()) {
					if (batch) {
						qrBuilder.setAmount(batchSize);

						Writable writable = output -> {
							try {
								QueryResponse qr = indexManager.query(qrBuilder.build());

								String header = buildHeaderForCSV(fields);
								output.write(header);
								output.flush();

								int count = 0;

								while (qr.getResultsList().size() > 0) {
									for (ScoredResult scoredResult : qr.getResultsList()) {
										Document doc = ResultHelper.getDocumentFromScoredResult(scoredResult);
										appendDocument(fields, null, output, doc);

										count++;
										if (count % 1000 == 0) {
											LOG.info("Docs processed so far: " + count);
										}
									}

									qrBuilder.setLastResult(qr.getLastResult());
									qr = indexManager.query(qrBuilder.build());

								}
							}
							catch (Exception e) {
								e.printStackTrace();
							}
						};

						LocalDateTime now = LocalDateTime.now();
						DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-H-mm-ss");

						return HttpResponse.ok(writable).status(ZuliaConstants.SUCCESS)
								.header("content-disposition", "attachment; filename = " + "zuliaDownload_" + now.format(formatter) + ".csv")
								.contentType(MediaType.APPLICATION_OCTET_STREAM);
					}
					else {
						QueryResponse qr = indexManager.query(qrBuilder.build());
						String response = getCSVDocumentResponse(fields, qr);
						return HttpResponse.ok(response).status(ZuliaConstants.SUCCESS).contentType(MediaType.TEXT_PLAIN + ";charset=utf-8");
					}
				}
				else if (facet != null && !facet.isEmpty() && rows == 0) {
					QueryResponse qr = indexManager.query(qrBuilder.build());

					StringBuilder response = new StringBuilder();
					response.append("facetName,facetKey,facetValue\n");

					for (FacetGroup facetGroup : qr.getFacetGroupList()) {
						for (FacetCount facetCount : facetGroup.getFacetCountList()) {
							response.append(facetGroup.getCountRequest().getFacetField().getLabel());
							response.append(",");
							response.append("\"").append(facetCount.getFacet()).append("\"");
							response.append(",");
							response.append(Long.valueOf(facetCount.getCount()));
							response.append("\n");
						}

					}
					return HttpResponse.ok(response.toString()).status(ZuliaConstants.SUCCESS).contentType(MediaType.TEXT_PLAIN + ";charset=utf-8");
				}
				else {
					return HttpResponse
							.ok("Please specify fields to be exported i.e. fl=title&fl=abstract or the facets to be exported i.e. facet=issn&facet=pubYear&rows=0")
							.status(ZuliaConstants.SUCCESS).contentType(MediaType.TEXT_PLAIN + ";charset=utf-8");
				}
			}
		}
		catch (Exception e) {
			LOG.log(Level.SEVERE, e.getMessage(), e);
			return HttpResponse.serverError(e.getClass().getSimpleName() + ":" + e.getMessage()).status(ZuliaConstants.INTERNAL_ERROR);
		}

	}

	private String buildHeaderForCSV(@Parameter(ZuliaConstants.FIELDS) List<String> fields) throws Exception {

		StringBuilder headerBuilder = new StringBuilder();
		fields.stream().filter(field -> !field.startsWith("-")).forEach(field -> headerBuilder.append(field).append(","));
		String headerOutput = headerBuilder.toString();
		return headerOutput.substring(0, headerOutput.length() - 1) + "\n";

	}

	private String getStandardResponse(QueryResponse qr, boolean strict, boolean cursor, boolean truncate) throws InvalidProtocolBufferException {
		StringBuilder responseBuilder = new StringBuilder();
		responseBuilder.append("{");
		responseBuilder.append("\"totalHits\": ");
		responseBuilder.append(qr.getTotalHits());
		if (cursor) {
			responseBuilder.append(",");
			responseBuilder.append("\"cursor\": \"");
			responseBuilder.append(CursorHelper.getUniqueSortedCursor(qr.getLastResult()));
			responseBuilder.append("\"");
		}

		if (!qr.getAnalysisResultList().isEmpty()) {
			responseBuilder.append(",");
			responseBuilder.append("\"analysis\": [");
			boolean first = true;
			for (AnalysisResult analysisResult : qr.getAnalysisResultList()) {
				if (first) {
					first = false;
				}
				else {
					responseBuilder.append(",");
				}
				responseBuilder.append("{");
				responseBuilder.append("\"field\": \"");
				responseBuilder.append(analysisResult.getAnalysisRequest().getField());
				responseBuilder.append("\"");

				responseBuilder.append(",");
				responseBuilder.append("\"terms\": [");

				JsonFormat.Printer printer = JsonFormat.printer();

				boolean firstInner = true;
				for (TermOrBuilder term : analysisResult.getTermsOrBuilderList()) {
					if (firstInner) {
						firstInner = false;
					}
					else {
						responseBuilder.append(",");
					}

					responseBuilder.append(printer.print(term));
				}
				responseBuilder.append("]");

				responseBuilder.append("}");
			}
			responseBuilder.append("]");
		}

		if (!qr.getResultsList().isEmpty()) {

			JsonFormat.Printer printer = JsonFormat.printer();

			responseBuilder.append(",");
			responseBuilder.append("\"results\": [");
			boolean first = true;
			for (ScoredResult sr : qr.getResultsList()) {
				if (first) {
					first = false;
				}
				else {
					responseBuilder.append(",");
				}
				responseBuilder.append("{");
				responseBuilder.append("\"id\": ");
				responseBuilder.append("\"").append(sr.getUniqueId()).append("\"");
				responseBuilder.append(",");
				if (!Double.isNaN(sr.getScore())) {
					responseBuilder.append("\"score\": ");
					responseBuilder.append(sr.getScore());
					responseBuilder.append(",");
				}
				responseBuilder.append("\"indexName\": ");
				responseBuilder.append("\"").append(sr.getIndexName()).append("\"");

				if (sr.hasResultDocument()) {
					responseBuilder.append(",");

					Document document = ResultHelper.getDocumentFromResultDocument(sr.getResultDocument());
					if (document != null) {
						responseBuilder.append("\"document\": ");

						if (strict) {
							responseBuilder.append(document.toJson());
						}
						else {
							if (truncate) {
								truncateDocumentValues(document);
							}

							responseBuilder.append(document.toJson(JsonWriterSettings.builder().indent(true).build()));
						}
					}

				}

				if (sr.getHighlightResultCount() > 0) {
					responseBuilder.append(",");

					responseBuilder.append("\"highlights\": [");
					boolean firstHighlightResult = true;
					for (HighlightResult hr : sr.getHighlightResultList()) {
						if (firstHighlightResult) {
							firstHighlightResult = false;
						}
						else {
							responseBuilder.append(",");
						}
						responseBuilder.append(printer.print(hr));
					}
					responseBuilder.append("]");

				}

				if (sr.getAnalysisResultCount() > 0) {
					responseBuilder.append(",");

					responseBuilder.append("\"analysis\": [");
					boolean firstAnalysisResult = true;
					for (AnalysisResult ar : sr.getAnalysisResultList()) {
						if (firstAnalysisResult) {
							firstAnalysisResult = false;
						}
						else {
							responseBuilder.append(",");
						}
						responseBuilder.append(printer.print(ar));
					}
					responseBuilder.append("]");

				}

				responseBuilder.append("}");
			}
			responseBuilder.append("]");
		}

		if (!qr.getFacetGroupList().isEmpty()) {
			responseBuilder.append(",");
			responseBuilder.append("\"facets\": [");
			boolean first = true;
			for (FacetGroup facetGroup : qr.getFacetGroupList()) {
				if (first) {
					first = false;
				}
				else {
					responseBuilder.append(",");
				}
				responseBuilder.append("{");
				responseBuilder.append("\"field\": \"");
				responseBuilder.append(facetGroup.getCountRequest().getFacetField().getLabel());
				responseBuilder.append("\"");
				if (facetGroup.getPossibleMissing()) {
					responseBuilder.append(",");
					responseBuilder.append("\"maxPossibleMissing\": ");
					responseBuilder.append(facetGroup.getMaxValuePossibleMissing());
				}
				responseBuilder.append(",");
				responseBuilder.append("\"values\": [");

				JsonFormat.Printer printer = JsonFormat.printer();

				boolean firstInner = true;
				for (FacetCount facetCount : facetGroup.getFacetCountList()) {
					if (firstInner) {
						firstInner = false;
					}
					else {
						responseBuilder.append(",");
					}

					responseBuilder.append(printer.print(facetCount));
				}
				responseBuilder.append("]");

				responseBuilder.append("}");
			}
			responseBuilder.append("]");
		}

		responseBuilder.append("}");

		return responseBuilder.toString();
	}

	private void truncateDocumentValues(Document document) {
		for (String key : document.keySet()) {
			if (document.get(key) instanceof Document) {
				truncateDocumentValues((Document) document.get(key));
			}
			if (document.get(key) instanceof String) {
				String value = (String) document.get(key);
				if (value.length() > 750) {
					document.put(key, value.substring(0, 750) + "...truncated for UI...");
				}
			}
			if (document.get(key) instanceof List) {
				List<?> o = (List<?>) document.get(key);
				if (!o.isEmpty()) {
					if (o.get(0) instanceof Document) {
						for (Object oDoc : o) {
							Document subDoc = (Document) oDoc;
							for (String subKey : subDoc.keySet()) {
								Object subValue = subDoc.get(subKey);
								if (subValue instanceof List) {
									if (!((List<?>) subValue).isEmpty()) {
										if (((List<?>) subValue).get(0) instanceof String) {
											if (((List<String>) subValue).size() > 10) {
												List<String> value = ((List<String>) subValue).subList(0, 10);
												value.add("list truncated for UI");
												subDoc.put(subKey, value);
											}
										}
									}
								}
							}
						}
					}
					else if (o.size() > 10 && o.get(0) instanceof String) {
						List<String> value = ((List<String>) o).subList(0, 10);
						value.add("list truncated for UI");
						document.put(key, value);
					}
				}
			}
		}
	}

	private String getCSVDocumentResponse(List<String> fields, QueryResponse qr) throws Exception {
		StringBuilder responseBuilder = new StringBuilder();

		// headersBuilder
		String header = buildHeaderForCSV(fields);
		responseBuilder.append(header);

		// records
		qr.getResultsList().stream().filter(ScoredResult::hasResultDocument).forEach(sr -> {
			Document document = ResultHelper.getDocumentFromResultDocument(sr.getResultDocument());
			try {
				appendDocument(fields, responseBuilder, null, document);
			}
			catch (Exception e) {
				LOG.log(Level.SEVERE, e.getMessage(), e);
			}
		});

		return responseBuilder.toString();
	}

	private void appendDocument(List<String> fields, StringBuilder responseBuilder, Writer outputStream, Document document) throws Exception {
		int i = 0;
		if (responseBuilder == null) {
			responseBuilder = new StringBuilder();
		}
		for (String field : fields) {
			Object obj = ResultHelper.getValueFromMongoDocument(document, field);
			if (obj != null) {
				if (obj instanceof List) {
					List value = (List) obj;

					if (!value.isEmpty()) {
						responseBuilder.append("\"");

						boolean first = true;
						for (Object o : value) {

							if (first) {
								first = false;
							}
							else {
								responseBuilder.append(";");
							}

							if (o instanceof String) {

								String item = (String) o;

								if (item.contains("\"")) {
									item = item.replace("\"", "\"\"");
								}

								responseBuilder.append(item);

							}
							else if (o instanceof Document) {
								responseBuilder.append(((Document) o).toJson().replace("\"", "\"\""));
							}
							else {
								responseBuilder.append(o.toString());
							}
						}
						responseBuilder.append("\"");
					}

				}
				else if (obj instanceof Date) {
					Date value = (Date) obj;
					responseBuilder.append(value.toString());
				}
				else if (obj instanceof Number) {
					Number value = (Number) obj;
					responseBuilder.append(value);
				}
				else if (obj instanceof Boolean) {
					Boolean value = (Boolean) obj;
					responseBuilder.append(value);
				}
				else if (obj instanceof Document) {
					responseBuilder.append("\"");
					responseBuilder.append(((Document) obj).toJson().replace("\"", "\"\""));
					responseBuilder.append("\"");
				}
				else {
					String value = (String) obj;
					if (value.contains(",") || value.contains(" ") || value.contains("\"") || value.contains("\n")) {
						responseBuilder.append("\"");
						responseBuilder.append(value.replace("\"", "\"\""));
						responseBuilder.append("\"");
					}
					else {
						responseBuilder.append(value);
					}
				}

			}

			i++;

			if (i < fields.size()) {
				responseBuilder.append(",");
			}

		}
		responseBuilder.append("\n");

		if (outputStream != null) {
			outputStream.write(responseBuilder.toString());
			outputStream.flush();
		}
	}

}