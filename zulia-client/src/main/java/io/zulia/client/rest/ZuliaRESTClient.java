package io.zulia.client.rest;

import com.google.protobuf.util.JsonFormat;
import io.zulia.ZuliaRESTConstants;
import io.zulia.client.rest.options.SearchRest;
import io.zulia.client.rest.options.TermsRestOptions;
import io.zulia.message.ZuliaQuery;
import io.zulia.message.ZuliaServiceOuterClass.RestIndexSettingsResponse;
import io.zulia.rest.dto.AssociatedMetadataDTO;
import io.zulia.rest.dto.FieldsDTO;
import io.zulia.rest.dto.IndexesResponseDTO;
import io.zulia.rest.dto.NodesResponseDTO;
import io.zulia.rest.dto.SearchResultsDTO;
import io.zulia.rest.dto.StatsDTO;
import io.zulia.rest.dto.TermsResponseDTO;
import kong.unirest.core.GenericType;
import kong.unirest.core.GetRequest;
import kong.unirest.core.HttpResponse;
import kong.unirest.core.MultipartBody;
import kong.unirest.core.Unirest;
import kong.unirest.core.UnirestInstance;
import kong.unirest.core.json.JSONArray;
import org.bson.Document;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

public class ZuliaRESTClient implements AutoCloseable {

	private final UnirestInstance unirestInstance;

	public ZuliaRESTClient(String server, int restPort) {
		this("http://" + server + ":" + restPort);
	}

	public ZuliaRESTClient(String url) {
		unirestInstance = Unirest.spawnInstance();
		unirestInstance.config().defaultBaseUrl(url);
	}

	public IndexesResponseDTO getIndexes() {
		return unirestInstance.get(ZuliaRESTConstants.INDEXES_URL).asObject(IndexesResponseDTO.class).ifFailure(new FailureHandler<>()).getBody();
	}

	public RestIndexSettingsResponse getIndex(String indexName) throws Exception {
		String json = unirestInstance.get(ZuliaRESTConstants.INDEXES_URL + "/{indexName}").routeParam("indexName", indexName).asString()
				.ifFailure(new FailureHandler<>()).getBody();
		RestIndexSettingsResponse.Builder builder = RestIndexSettingsResponse.newBuilder();
		JsonFormat.parser().merge(json, builder);
		return builder.build();
	}

	public NodesResponseDTO getNodes(boolean active) throws Exception {
		return unirestInstance.get(ZuliaRESTConstants.NODES_URL).queryString("active", active).asObject(NodesResponseDTO.class)
				.ifFailure(new FailureHandler<>()).getBody();
	}

	public FieldsDTO getFields(String indexName) {
		return unirestInstance.get(ZuliaRESTConstants.FIELDS_URL).queryString(ZuliaRESTConstants.INDEX, indexName).asObject(FieldsDTO.class)
				.ifFailure(new FailureHandler<>()).getBody();
	}

	@Override
	public void close() throws Exception {
		unirestInstance.close();
	}

	public Document fetchRecord(String indexName, String uniqueId) {
		String json = unirestInstance.get(ZuliaRESTConstants.FETCH_URL + "/{indexName}/{uniqueId}").routeParam("indexName", indexName)
				.routeParam("uniqueId", uniqueId).asString().ifFailure(new FailureHandler<>()).getBody();
		return Document.parse(json);
	}

	public StatsDTO getStats() {
		return unirestInstance.get(ZuliaRESTConstants.STATS_URL).asObject(StatsDTO.class).ifFailure(new FailureHandler<>()).getBody();
	}

	public TermsResponseDTO getTerms(String indexName, String fieldName) {
		return getTerms(indexName, fieldName, null);
	}

	public TermsResponseDTO getTerms(String indexName, String fieldName, TermsRestOptions termsOptions) {
		return unirestInstance.get(ZuliaRESTConstants.TERMS_URL).queryString(ZuliaRESTConstants.INDEX, indexName)
				.queryString(ZuliaRESTConstants.FIELDS, fieldName).queryString(termsOptions != null ? termsOptions.getParameters() : null)
				.asObject(TermsResponseDTO.class).ifFailure(new FailureHandler<>()).getBody();
	}

	public void storeAssociated(String indexName, String uniqueId, String fileName, Document metadata, byte[] bytes) throws Exception {
		storeAssociated(indexName, uniqueId, fileName, metadata, new ByteArrayInputStream(bytes));
	}

	public void storeAssociated(String indexName, String uniqueId, String fileName, Document metadata, File file) throws Exception {
		storeAssociated(indexName, uniqueId, fileName, metadata, new FileInputStream(file));
	}

	public void storeAssociated(String indexName, String uniqueId, String fileName, Document metadata, InputStream inputStream) throws Exception {
		storeAssociated(indexName, uniqueId, fileName, metadata, inputStream, true);
	}

	public void storeAssociated(String indexName, String uniqueId, String fileName, Document metadata, InputStream inputStream, boolean closeStream)
			throws Exception {

		try {
			MultipartBody multipartBody = unirestInstance.post(ZuliaRESTConstants.ASSOCIATED_URL + "/{indexName}/{uniqueId}/{fileName}")
					.routeParam("indexName", indexName).routeParam("uniqueId", uniqueId).routeParam("fileName", fileName).multiPartContent();

			if (metadata != null) {
				multipartBody.field("metaJson", metadata.toJson());
			}
			multipartBody.field("file", inputStream, fileName);
			HttpResponse<String> response = multipartBody.asString();
			if (!response.isSuccess()) {
				throw new Exception(response.getBody());
			}
		}
		finally {
			if (closeStream) {
				inputStream.close();
			}
		}

	}

	public void fetchAssociated(String indexName, String uniqueId, String fileName, File destination) throws Exception {
		fetchAssociated(indexName, uniqueId, fileName, new FileOutputStream(destination));
	}

	public void fetchAssociated(String indexName, String uniqueId, String fileName, OutputStream destination) throws Exception {
		fetchAssociated(indexName, uniqueId, fileName, destination, true);
	}

	public void fetchAssociated(String indexName, String uniqueId, String fileName, OutputStream destination, boolean closeStream) throws Exception {
		try {
			GetRequest request = unirestInstance.get(ZuliaRESTConstants.ASSOCIATED_URL + "/{indexName}/{uniqueId}/{fileName}/file")
					.routeParam("indexName", indexName).routeParam("uniqueId", uniqueId).routeParam("fileName", fileName);

			request.thenConsume(rawResponse -> {
				try {
					rawResponse.getContent().transferTo(destination);
				}
				catch (IOException e) {
					throw new RuntimeException(e);
				}
			});
		}
		finally {
			if (closeStream) {
				destination.close();
			}
		}

	}

	public Document fetchAssociatedMetadata(String indexName, String uniqueId, String fileName) {
		String json = unirestInstance.get(ZuliaRESTConstants.ASSOCIATED_URL + "/{indexName}/{uniqueId}/{fileName}/metadata").routeParam("indexName", indexName)
				.routeParam("uniqueId", uniqueId).routeParam("fileName", fileName).asString().ifFailure(new FailureHandler<>()).getBody();
		return Document.parse(json);
	}

	public List<String> fetchAssociatedFilenamesForId(String indexName, String uniqueId) {
		JSONArray json = unirestInstance.get(ZuliaRESTConstants.ASSOCIATED_URL + "/{indexName}/{uniqueId}/filenames").routeParam("indexName", indexName)
				.routeParam("uniqueId", uniqueId).asJson().getBody().getObject().getJSONArray("filenames");
		List<String> filenames = new ArrayList<>();
		for (int i = 0; i < json.length(); i++) {
			filenames.add(json.getString(i));
		}
		return filenames;
	}

	public void fetchAssociatedBundle(String indexName, String uniqueId, File destination) throws Exception {
		fetchAssociatedBundle(indexName, uniqueId, new FileOutputStream(destination));
	}

	public void fetchAssociatedBundle(String indexName, String uniqueId, OutputStream destination) throws Exception {
		fetchAssociatedBundle(indexName, uniqueId, destination, true);
	}

	public void fetchAssociatedBundle(String indexName, String uniqueId, OutputStream destination, boolean closeStream) throws IOException {
		try {
			GetRequest request = unirestInstance.get(ZuliaRESTConstants.ASSOCIATED_URL + "/{indexName}/{uniqueId}/bundle").routeParam("indexName", indexName)
					.routeParam("uniqueId", uniqueId);

			request.thenConsume(rawResponse -> {
				try {
					rawResponse.getContent().transferTo(destination);
				}
				catch (IOException e) {
					throw new RuntimeException(e);
				}
			});
		}
		finally {
			if (closeStream) {
				destination.close();
			}
		}
	}

	public List<AssociatedMetadataDTO> fetchAssociatedForIndex(String indexName) {
		return fetchAssociatedForIndex(indexName, null);
	}

	public List<AssociatedMetadataDTO> fetchAssociatedForIndex(String indexName, Document query) {
		GetRequest request = unirestInstance.get(ZuliaRESTConstants.ASSOCIATED_URL + "/{indexName}/all").routeParam("indexName", indexName);

		if (query != null) {
			request = request.queryString(ZuliaRESTConstants.QUERY, query.toJson());
		}
		return request.asObject(new GenericType<List<AssociatedMetadataDTO>>() {
		}).getBody();

	}

	public SearchResultsDTO search(SearchRest searchRest) {
		GetRequest request = unirestInstance.get(ZuliaRESTConstants.QUERY_URL).queryString(ZuliaRESTConstants.INDEX, searchRest.getIndexNames());
		if (searchRest.getRows() > 0) {
			request = request.queryString(ZuliaRESTConstants.ROWS, searchRest.getRows());
		}
		if (searchRest.getQuery() != null) {
			request = request.queryString(ZuliaRESTConstants.QUERY, searchRest.getQuery());
		}
		if (searchRest.getMm() != null) {
			request = request.queryString(ZuliaRESTConstants.MIN_MATCH, searchRest.getMm());
		}
		if (searchRest.getCursor() != null) {
			request = request.queryString(ZuliaRESTConstants.CURSOR, searchRest.getCursor());
		}
		if (searchRest.getDefaultOperator() != null) {
			request = request.queryString(ZuliaRESTConstants.DEFAULT_OP,
					searchRest.getDefaultOperator().equals(ZuliaQuery.Query.Operator.AND) ? ZuliaRESTConstants.AND : ZuliaRESTConstants.OR);
		}

		if (searchRest.getFacet() != null && !searchRest.getFacet().isEmpty()) {
			request = request.queryString(ZuliaRESTConstants.FACET, searchRest.getFacet());
		}
		if (searchRest.getFields() != null && !searchRest.getFields().isEmpty()) {
			request = request.queryString(ZuliaRESTConstants.FIELDS, searchRest.getFields());
		}
		if (searchRest.getDrillDowns() != null && !searchRest.getDrillDowns().isEmpty()) {
			request = request.queryString(ZuliaRESTConstants.DRILL_DOWN, searchRest.getDrillDowns());
		}
		if (searchRest.getFilterQueries() != null && !searchRest.getFilterQueries().isEmpty()) {
			request = request.queryString(ZuliaRESTConstants.FILTER_QUERY, searchRest.getFilterQueries());
		}
		if (searchRest.getQueryFields() != null && !searchRest.getQueryFields().isEmpty()) {
			request = request.queryString(ZuliaRESTConstants.QUERY_FIELD, searchRest.getQueryFields());
		}
		if (searchRest.getSort() != null && !searchRest.getSort().isEmpty()) {
			request = request.queryString(ZuliaRESTConstants.SORT, searchRest.getSort());
		}
		if (searchRest.getHighlights() != null && !searchRest.getHighlights().isEmpty()) {
			request = request.queryString(ZuliaRESTConstants.HIGHLIGHT, searchRest.getHighlights());
		}

		return request.asObject(SearchResultsDTO.class).ifFailure(new FailureHandler<>()).getBody();
	}

}
