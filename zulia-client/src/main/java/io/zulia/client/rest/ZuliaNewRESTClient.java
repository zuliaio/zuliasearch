package io.zulia.client.rest;

import com.google.protobuf.util.JsonFormat;
import io.zulia.ZuliaRESTConstants;
import io.zulia.client.rest.options.TermsRestOptions;
import io.zulia.message.ZuliaServiceOuterClass.RestIndexSettingsResponse;
import io.zulia.rest.dto.FieldsDTO;
import io.zulia.rest.dto.IndexesResponseDTO;
import io.zulia.rest.dto.NodesResponseDTO;
import io.zulia.rest.dto.StatsDTO;
import io.zulia.rest.dto.TermsResponseDTO;
import kong.unirest.core.GetRequest;
import kong.unirest.core.HttpResponse;
import kong.unirest.core.MultipartBody;
import kong.unirest.core.Unirest;
import kong.unirest.core.UnirestInstance;
import org.bson.Document;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

public class ZuliaNewRESTClient implements AutoCloseable {

	private final UnirestInstance unirestInstance;

	public ZuliaNewRESTClient(String url) {
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

	public void storeAssociated(String uniqueId, String indexName, String fileName, Document metadata, byte[] bytes) throws Exception {

		MultipartBody multipartBody = buildStoreAssociatedPost(uniqueId, indexName, fileName, metadata);
		multipartBody.field("file", bytes, fileName);
		HttpResponse<String> response = multipartBody.asString();
		if (!response.isSuccess()) {
			throw new Exception(response.getBody());
		}

	}

	public void storeAssociated(String uniqueId, String indexName, String fileName, Document metadata, File file) throws Exception {

		MultipartBody multipartBody = buildStoreAssociatedPost(uniqueId, indexName, fileName, metadata);
		multipartBody.field("file", file);
		HttpResponse<String> response = multipartBody.asString();
		if (!response.isSuccess()) {
			throw new Exception(response.getBody());
		}

	}

	private MultipartBody buildStoreAssociatedPost(String uniqueId, String indexName, String fileName, Document metadata) {
		MultipartBody multipartBody = unirestInstance.post(ZuliaRESTConstants.ASSOCIATED_DOCUMENTS_URL).field("id", uniqueId).field("fileName", fileName)
				.field("indexName", indexName);

		if (metadata != null) {
			multipartBody.field("metaJson", metadata.toJson());
		}
		return multipartBody;
	}

	public void fetchAssociated(String uniqueId, String indexName, String fileName, OutputStream destination, boolean closeStream) throws Exception {
		try {
			GetRequest request = Unirest.get(ZuliaRESTConstants.ASSOCIATED_DOCUMENTS_URL).queryString(ZuliaRESTConstants.ID, uniqueId)
					.queryString(ZuliaRESTConstants.FILE_NAME, fileName).queryString(ZuliaRESTConstants.INDEX, indexName);

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

}
