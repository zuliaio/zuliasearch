package io.zulia.server.rest.controllers;

import com.google.common.base.Joiner;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;
import io.micronaut.context.annotation.Parameter;
import io.micronaut.core.annotation.Nullable;
import io.micronaut.core.io.Writable;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.MutableHttpResponse;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;
import io.micronaut.http.annotation.Produces;
import io.micronaut.http.annotation.QueryValue;
import io.micronaut.scheduling.TaskExecutors;
import io.micronaut.scheduling.annotation.ExecuteOn;
import io.zulia.ZuliaRESTConstants;
import io.zulia.rest.dto.*;
import io.zulia.server.exceptions.WrappedCheckedException;
import io.zulia.server.index.ZuliaIndexManager;
import io.zulia.server.util.ZuliaNodeProvider;
import io.zulia.util.CursorHelper;
import io.zulia.util.ResultHelper;
import io.zulia.util.document.DocumentHelper;
import org.bson.Document;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Writer;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import static io.zulia.message.ZuliaBase.Similarity;
import static io.zulia.message.ZuliaQuery.*;
import static io.zulia.message.ZuliaServiceOuterClass.QueryRequest;
import static io.zulia.message.ZuliaServiceOuterClass.QueryResponse;

/**
 * Created by Payam Meyer on 8/7/17.
 *
 * @author pmeyer
 */
@Controller(ZuliaRESTConstants.QUERY_URL)
@ExecuteOn(TaskExecutors.BLOCKING)
public class QueryController {

	private final static Logger LOG = LoggerFactory.getLogger(QueryController.class);

	@Get
	@Produces(ZuliaRESTConstants.UTF8_JSON)
	public SearchResultsDTO get(@QueryValue(ZuliaRESTConstants.INDEX) List<String> indexName,
			@QueryValue(value = ZuliaRESTConstants.QUERY, defaultValue = "*:*") String query,
			@Nullable @QueryValue(ZuliaRESTConstants.QUERY_FIELD) List<String> queryFields,
			@Nullable @QueryValue(ZuliaRESTConstants.FILTER_QUERY) List<String> filterQueries,
			@Nullable @QueryValue(ZuliaRESTConstants.QUERY_JSON) List<String> queryJsonList,
			@Nullable @QueryValue(ZuliaRESTConstants.FIELDS) List<String> fields,
			@QueryValue(value = ZuliaRESTConstants.FETCH, defaultValue = "true") Boolean fetch,
			@QueryValue(value = ZuliaRESTConstants.ROWS, defaultValue = "0") Integer rows, @Nullable @QueryValue(ZuliaRESTConstants.FACET) List<String> facet,
			@Nullable @QueryValue(ZuliaRESTConstants.DRILL_DOWN) List<String> drillDowns,
			@Nullable @QueryValue(ZuliaRESTConstants.DEFAULT_OP) String defaultOperator, @Nullable @QueryValue(ZuliaRESTConstants.SORT) List<String> sort,
			@Nullable @QueryValue(ZuliaRESTConstants.MIN_MATCH) Integer mm, @Nullable @QueryValue(ZuliaRESTConstants.SIMILARITY) List<String> similarity,
			@QueryValue(value = ZuliaRESTConstants.DEBUG, defaultValue = "false") Boolean debug,
			@QueryValue(value = ZuliaRESTConstants.DONT_CACHE, defaultValue = "true") Boolean dontCache,
			@Nullable @QueryValue(ZuliaRESTConstants.START) Integer start, @Nullable @QueryValue(ZuliaRESTConstants.HIGHLIGHT) List<String> highlightList,
			@Nullable @QueryValue(ZuliaRESTConstants.HIGHLIGHT_JSON) List<String> highlightJsonList,
			@Nullable @QueryValue(ZuliaRESTConstants.ANALYZE_JSON) List<String> analyzeJsonList, @Nullable @QueryValue(ZuliaRESTConstants.CURSOR) String cursor,
			@QueryValue(value = ZuliaRESTConstants.TRUNCATE, defaultValue = "false") Boolean truncate,
			@Nullable @QueryValue(ZuliaRESTConstants.REALTIME) Boolean realtime) throws Exception {

		ZuliaIndexManager indexManager = ZuliaNodeProvider.getZuliaNode().getIndexManager();

		QueryRequest.Builder qrBuilder = buildQueryRequest(indexName, query, queryFields, filterQueries, queryJsonList, fields, fetch, rows, facet, drillDowns,
				defaultOperator, sort, mm, similarity, debug, dontCache, start, highlightList, highlightJsonList, analyzeJsonList, cursor, realtime);
		QueryResponse qr = indexManager.query(qrBuilder.build());
		return getJsonResponse(qr, cursor != null, truncate);

	}

	@Get("/csv")
	@Produces(ZuliaRESTConstants.UTF8_CSV)
	public HttpResponse<?> getCSV(@QueryValue(ZuliaRESTConstants.INDEX) List<String> indexName,
			@QueryValue(value = ZuliaRESTConstants.QUERY, defaultValue = "*:*") String query,
			@Nullable @QueryValue(ZuliaRESTConstants.QUERY_FIELD) List<String> queryFields,
			@Nullable @QueryValue(ZuliaRESTConstants.FILTER_QUERY) List<String> filterQueries,
			@Nullable @QueryValue(ZuliaRESTConstants.QUERY_JSON) List<String> queryJsonList,
			@Nullable @QueryValue(ZuliaRESTConstants.FIELDS) List<String> fields,
			@QueryValue(value = ZuliaRESTConstants.FETCH, defaultValue = "true") Boolean fetch,
			@QueryValue(value = ZuliaRESTConstants.ROWS, defaultValue = "0") Integer rows, @Nullable @QueryValue(ZuliaRESTConstants.FACET) List<String> facet,
			@Nullable @QueryValue(ZuliaRESTConstants.DRILL_DOWN) List<String> drillDowns,
			@Nullable @QueryValue(ZuliaRESTConstants.DEFAULT_OP) String defaultOperator, @Nullable @QueryValue(ZuliaRESTConstants.SORT) List<String> sort,
			@Nullable @QueryValue(ZuliaRESTConstants.MIN_MATCH) Integer mm, @Nullable @QueryValue(ZuliaRESTConstants.SIMILARITY) List<String> similarity,
			@QueryValue(value = ZuliaRESTConstants.DEBUG, defaultValue = "false") Boolean debug,
			@QueryValue(value = ZuliaRESTConstants.DONT_CACHE, defaultValue = "true") Boolean dontCache,
			@Nullable @QueryValue(ZuliaRESTConstants.START) Integer start, @Nullable @QueryValue(ZuliaRESTConstants.HIGHLIGHT) List<String> highlightList,
			@Nullable @QueryValue(ZuliaRESTConstants.HIGHLIGHT_JSON) List<String> highlightJsonList,
			@Nullable @QueryValue(ZuliaRESTConstants.ANALYZE_JSON) List<String> analyzeJsonList,
			@QueryValue(value = ZuliaRESTConstants.BATCH, defaultValue = "false") Boolean batch,
			@QueryValue(value = ZuliaRESTConstants.BATCH_SIZE, defaultValue = "500") Integer batchSize,
			@Nullable @QueryValue(ZuliaRESTConstants.CURSOR) String cursor,
			@Nullable @QueryValue(ZuliaRESTConstants.REALTIME) Boolean realtime) throws Exception {

		ZuliaIndexManager indexManager = ZuliaNodeProvider.getZuliaNode().getIndexManager();

		QueryRequest.Builder qrBuilder = buildQueryRequest(indexName, query, queryFields, filterQueries, queryJsonList, fields, fetch, rows, facet, drillDowns,
				defaultOperator, sort, mm, similarity, debug, dontCache, start, highlightList, highlightJsonList, analyzeJsonList, cursor, realtime);

		LocalDateTime now = LocalDateTime.now();
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-H-mm-ss");
		String fileName = "ZuliaDownload_" + Joiner.on("-").join(indexName) + "_" + now.format(formatter) + ".csv";
		if (fields != null && !fields.isEmpty()) {
			if (batch) {
				return getBatchStream(fields, batchSize, qrBuilder, indexManager, fileName);
			}
			else {
				QueryResponse qr = indexManager.query(qrBuilder.build());
				String response = getCSVDocumentResponse(fields, qr);
				return HttpResponse.ok(response).header("Content-Disposition", "attachment; filename=" + fileName)
						.contentType(MediaType.APPLICATION_OCTET_STREAM);
			}
		}
		else {
			throw new IllegalArgumentException("Please specify fields to be exported i.e. fl=title&fl=abstract");
		}
	}

	@Get("/facet")
	@Produces(ZuliaRESTConstants.UTF8_CSV)
	public HttpResponse<?> getFacets(@QueryValue(ZuliaRESTConstants.INDEX) List<String> indexName,
			@QueryValue(value = ZuliaRESTConstants.QUERY, defaultValue = "*:*") String query,
			@Nullable @QueryValue(ZuliaRESTConstants.QUERY_FIELD) List<String> queryFields,
			@Nullable @QueryValue(ZuliaRESTConstants.FILTER_QUERY) List<String> filterQueries,
			@Nullable @QueryValue(ZuliaRESTConstants.QUERY_JSON) List<String> queryJsonList,
			@Nullable @QueryValue(ZuliaRESTConstants.FIELDS) List<String> fields, @Nullable @QueryValue(ZuliaRESTConstants.FACET) List<String> facet,
			@Nullable @QueryValue(ZuliaRESTConstants.DRILL_DOWN) List<String> drillDowns,
			@Nullable @QueryValue(ZuliaRESTConstants.DEFAULT_OP) String defaultOperator, @Nullable @QueryValue(ZuliaRESTConstants.SORT) List<String> sort,
			@Nullable @QueryValue(ZuliaRESTConstants.MIN_MATCH) Integer mm, @QueryValue(value = ZuliaRESTConstants.DEBUG, defaultValue = "false") Boolean debug,
			@Nullable @QueryValue(ZuliaRESTConstants.START) Integer start, @Nullable @QueryValue(ZuliaRESTConstants.CURSOR) String cursor,
			@Nullable @QueryValue(ZuliaRESTConstants.REALTIME) Boolean realtime) throws Exception {

		ZuliaIndexManager indexManager = ZuliaNodeProvider.getZuliaNode().getIndexManager();

		QueryRequest.Builder qrBuilder = buildQueryRequest(indexName, query, queryFields, filterQueries, queryJsonList, fields, null, 0, facet, drillDowns,
				defaultOperator, sort, mm, null, debug, null, start, null, null, null, cursor, realtime);

		if (facet != null && !facet.isEmpty()) {
			String response = getFacetCSV(indexManager, qrBuilder);
			return HttpResponse.ok(response);
		}
		else {
			throw new IllegalArgumentException("Please specify facets to be exported i.e. facet=issn&facet=pubYear");
		}
	}

	private MutableHttpResponse<Writable> getBatchStream(List<String> fields, Integer batchSize, QueryRequest.Builder qrBuilder, ZuliaIndexManager indexManager,
			String fileName) {
		qrBuilder.setAmount(batchSize);

		Writable writable = output -> {
			try {
				QueryResponse qr = indexManager.query(qrBuilder.build());

				String header = buildHeaderForCSV(fields);
				output.write(header);
				output.flush();

				int count = 0;

				while (!qr.getResultsList().isEmpty()) {
					for (ScoredResult scoredResult : qr.getResultsList()) {
						Document doc = ResultHelper.getDocumentFromScoredResult(scoredResult);
						appendDocument(fields, null, output, doc);

						count++;
						if (count % 1000 == 0) {
							LOG.info("Docs processed so far: {}", count);
						}
					}

					qrBuilder.setLastResult(qr.getLastResult());
					qr = indexManager.query(qrBuilder.build());

				}
			}
			catch (Exception e) {
				throw new WrappedCheckedException(e);
			}
		};

		return HttpResponse.ok(writable).header("Content-Disposition", "attachment; filename=" + fileName).contentType(MediaType.APPLICATION_OCTET_STREAM);
	}

	@NotNull
	private static String getFacetCSV(ZuliaIndexManager indexManager, QueryRequest.Builder qrBuilder) throws Exception {
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
		return response.toString();
	}

	@NotNull
	private static QueryRequest.Builder buildQueryRequest(List<String> indexName, String query, List<String> queryFields, List<String> filterQueries,
			List<String> queryJsonList, List<String> fields, Boolean fetch, Integer rows, List<String> facet, List<String> drillDowns, String defaultOperator,
			List<String> sort, Integer mm, List<String> similarity, Boolean debug, Boolean dontCache, Integer start, List<String> highlightList,
			List<String> highlightJsonList, List<String> analyzeJsonList, String cursor, Boolean realtime) {
		QueryRequest.Builder qrBuilder = QueryRequest.newBuilder().addAllIndex(indexName);
		if (cursor != null) {
			if (!cursor.equals("0")) {
				qrBuilder.setLastResult(CursorHelper.getLastResultFromCursor(cursor));
			}
			if (sort == null || sort.isEmpty()) {
				throw new IllegalArgumentException("Sort on unique value or value combination is required to use a cursor (i.e. id or title,id)");
			}
		}

		if (realtime != null) {
			qrBuilder.setRealtime(realtime);
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
		if (queryFields != null) {
			mainQueryBuilder.addAllQf(queryFields);
		}
		if (defaultOperator != null) {
			if (defaultOperator.equalsIgnoreCase(ZuliaRESTConstants.AND)) {
				mainQueryBuilder.setDefaultOp(Query.Operator.AND);
			}
			else if (defaultOperator.equalsIgnoreCase(ZuliaRESTConstants.OR)) {
				mainQueryBuilder.setDefaultOp(Query.Operator.OR);
			}
			else {
				throw new IllegalArgumentException("Invalid default operator " + defaultOperator);
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
						throw new IllegalArgumentException("Unknown similarity type " + simType );
					}

					qrBuilder.addFieldSimilarity(fieldSimilarity);
				}
				else {
					throw new IllegalArgumentException("Similarity " + sim + " should be in the form field:simType");
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
					throw new IllegalArgumentException("Failed to parse query json: " + e.getClass().getSimpleName() + ":" + e.getMessage());
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
					throw new IllegalArgumentException("Failed to parse highlight json: " + e.getClass().getSimpleName() + ":" + e.getMessage());
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
					throw new IllegalArgumentException("Failed to parse analyzer json: " + e.getClass().getSimpleName() + ":" + e.getMessage());
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
						throw new IllegalArgumentException("Invalid facet count " + countString + " for facet " + f);
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

		if (drillDowns != null && !drillDowns.isEmpty()) {

			Map<String, Set<String>> labelToValues = new HashMap<>();

			for (String drillDown : drillDowns) {
				if (drillDown.contains(":")) {
					String field = drillDown.substring(0, drillDown.indexOf(":"));
					String value = drillDown.substring(drillDown.indexOf(":") + 1);
					labelToValues.computeIfAbsent(field, s -> new HashSet<>()).add(value);
				}
			}

			for (String label : labelToValues.keySet()) {
				Set<String> values = labelToValues.get(label);
				List<Facet> facetValues = values.stream().map(s -> Facet.newBuilder().setValue(s).build()).toList();
				frBuilder.addDrillDown(DrillDown.newBuilder().setLabel(label).addAllFacetValue(facetValues));
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

					if ("-1".equals(sortDir) || ZuliaRESTConstants.DESC.equalsIgnoreCase(sortDir)) {
						fieldSort.setDirection(FieldSort.Direction.DESCENDING);
					}
					else if ("1".equals(sortDir) || ZuliaRESTConstants.ASC.equalsIgnoreCase(sortDir)) {
						fieldSort.setDirection(FieldSort.Direction.ASCENDING);
					}
					else {
						throw new IllegalArgumentException(
								"Invalid sort direction <" + sortDir + "> for field <" + sortField + ">.  Expecting -1/1 or DESC/ASC");
					}
				}
				fieldSort.setSortField(sortField);
				sortRequest.addFieldSort(fieldSort);
			}
			qrBuilder.setSortRequest(sortRequest);
		}
		qrBuilder.setAmount(rows);
		return qrBuilder;
	}

	private String buildHeaderForCSV(@Parameter(ZuliaRESTConstants.FIELDS) List<String> fields) {

		StringBuilder headerBuilder = new StringBuilder();
		fields.stream().filter(field -> !field.startsWith("-")).forEach(field -> headerBuilder.append(field).append(","));
		String headerOutput = headerBuilder.toString();
		return headerOutput.substring(0, headerOutput.length() - 1) + "\n";

	}

	private SearchResultsDTO getJsonResponse(QueryResponse qr, boolean cursor, boolean truncate) {

		SearchResultsDTO searchResultsDTO = new SearchResultsDTO();
		searchResultsDTO.setTotalHits(qr.getTotalHits());
		if (cursor) {
			searchResultsDTO.setCursor(CursorHelper.getUniqueSortedCursor(qr.getLastResult()));
		}

		if (!qr.getAnalysisResultList().isEmpty()) {
			List<AnalysisDTO> analysis = new ArrayList<>();
			for (AnalysisResult analysisResult : qr.getAnalysisResultList()) {
				AnalysisDTO analysisDTO = new AnalysisDTO(analysisResult.getAnalysisRequest().getField(),
						analysisResult.getTermsList().stream().map(TermDTO::fromTerm).toList());
				analysis.add(analysisDTO);
			}
			searchResultsDTO.setAnalysis(analysis);
		}

		if (!qr.getResultsList().isEmpty()) {
			List<ScoredResultDTO> results = new ArrayList<>();

			for (ScoredResult sr : qr.getResultsList()) {
				ScoredResultDTO scoredResultDTO = new ScoredResultDTO();
				scoredResultDTO.setId(sr.getUniqueId());
				if (!Double.isNaN(sr.getScore())) {
					scoredResultDTO.setScore(sr.getScore());
				}

				scoredResultDTO.setIndexName(sr.getIndexName());

				if (sr.hasResultDocument()) {
					Document document = ResultHelper.getDocumentFromResultDocument(sr.getResultDocument());
					if (document != null) {
						if (truncate) {
							truncateDocumentValues(document);
						}
						scoredResultDTO.setDocument(document);
					}
				}

				if (sr.getHighlightResultCount() > 0) {
					List<HighlightDTO> highlights = sr.getHighlightResultList().stream().map(HighlightDTO::fromHighlightResult).toList();
					scoredResultDTO.setHighlights(highlights);
				}

				if (sr.getAnalysisResultCount() > 0) {
					List<AnalysisDTO> analysis = sr.getAnalysisResultList().stream().map(AnalysisDTO::fromAnalysisResult).toList();
					searchResultsDTO.setAnalysis(analysis);
				}

				results.add(scoredResultDTO);
			}

			searchResultsDTO.setResults(results);
		}

		if (!qr.getFacetGroupList().isEmpty()) {

			List<FacetsDTO> facets = qr.getFacetGroupList().stream().map(FacetsDTO::fromFacetGroup).toList();
			searchResultsDTO.setFacets(facets);
		}

		return searchResultsDTO;
	}

	private void truncateDocumentValues(Document document) {
		for (String key : document.keySet()) {
			if (document.get(key) instanceof Document) {
				truncateDocumentValues((Document) document.get(key));
			}
			if (document.get(key) instanceof String) {
				String value = (String) document.get(key);
				if (value.length() > 750) {
					document.put(key, value.substring(0, 750) + "...[truncated]");
				}
			}
			if (document.get(key) instanceof List<?> o) {
				if (!o.isEmpty()) {
					if (o.getFirst() instanceof Document) {
						for (Object oDoc : o) {
							Document subDoc = (Document) oDoc;
							for (String subKey : subDoc.keySet()) {
								Object subValue = subDoc.get(subKey);
								if (subValue instanceof List) {
									if (!((List<?>) subValue).isEmpty()) {
										if (((List<?>) subValue).getFirst() instanceof String) {
											if (((List<String>) subValue).size() > 10) {
												List<String> value = ((List<String>) subValue).subList(0, 10);
												value.add("...[truncated]");
												subDoc.put(subKey, value);
											}
										}
									}
								}
							}
						}
					}
					else if (o.size() > 10 && o.getFirst() instanceof String) {
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
				LOG.error(e.getMessage(), e);
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
			Object obj = DocumentHelper.getValueFromMongoDocument(document, field);
			if (obj != null) {
				switch (obj) {
					case List<?> list -> {
						if (!list.isEmpty()) {
							responseBuilder.append("\"");

							boolean first = true;
							for (Object o : list) {

								if (first) {
									first = false;
								}
								else {
									responseBuilder.append(";");
								}

								if (o instanceof String str) {
									responseBuilder.append(CSVUtil.quoteForCSV(str));
								}
								else if (o instanceof Document doc) {
									responseBuilder.append(doc.toJson().replace("\"", "\"\""));
								}
								else {
									responseBuilder.append(o.toString());
								}
							}
							responseBuilder.append("\"");
						}
					}
					case Date d -> responseBuilder.append(d);
					case Number n -> responseBuilder.append(n);
					case Boolean bool -> responseBuilder.append(bool);
					case Document doc -> {
						responseBuilder.append("\"");
						responseBuilder.append((doc).toJson().replace("\"", "\"\""));
						responseBuilder.append("\"");
					}
					case String string -> {
						responseBuilder.append(CSVUtil.quoteForCSV(string));
					}
					default -> {

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