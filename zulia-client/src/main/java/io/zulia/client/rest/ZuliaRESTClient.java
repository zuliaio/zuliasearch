package io.zulia.client.rest;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.zulia.ZuliaConstants;
import io.zulia.util.HttpHelper;
import io.zulia.util.ZuliaUtil;
import okhttp3.MediaType;
import okhttp3.MultipartBody;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okio.BufferedSink;
import okio.Okio;
import org.bson.Document;

import java.io.File;
import java.io.OutputStream;
import java.net.URLConnection;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class ZuliaRESTClient {

	private static final Logger LOG = Logger.getLogger(ZuliaRESTClient.class.getName());
	private final String url;
	private final OkHttpClient client;

	public ZuliaRESTClient(String server, int restPort) {
		url = "http://" + server + ":" + restPort;

		client = new OkHttpClient().newBuilder().build();
		LOG.info("Created OkHttp client for url: " + url);
	}

	public void storeAssociated(String uniqueId, String indexName, String fileName, Document metadata, byte[] bytes) throws Exception {

		try {

			RequestBody body;
			if (metadata != null) {
				body = new MultipartBody.Builder().setType(MultipartBody.FORM).addFormDataPart("id", uniqueId).addFormDataPart("fileName", fileName)
						.addFormDataPart("indexName", indexName).addFormDataPart("metaJson", metadata.toJson())
						.addFormDataPart("file", fileName, RequestBody.create(bytes, MediaType.parse(URLConnection.guessContentTypeFromName(fileName))))
						.build();
			}
			else {
				body = new MultipartBody.Builder().setType(MultipartBody.FORM).addFormDataPart("id", uniqueId).addFormDataPart("fileName", fileName)
						.addFormDataPart("indexName", indexName)
						.addFormDataPart("file", fileName, RequestBody.create(bytes, MediaType.parse(URLConnection.guessContentTypeFromName(fileName))))
						.build();
			}

			Request request = new Request.Builder().url(url + ZuliaConstants.ASSOCIATED_DOCUMENTS_URL).method("POST", body).build();
			Response response = client.newCall(request).execute();
			response.close();

		}
		catch (Exception e) {
			if (e.getMessage().startsWith("Out of size:")) {
				LOG.log(Level.WARNING, "Failed to store file <" + fileName + "> due to mismatch size.");
			}
			else {
				LOG.log(Level.SEVERE, "Failed to store file <" + fileName + ">", e);
				throw e;
			}
		}

	}

	public void fetchAssociated(String uniqueId, String indexName, String fileName, OutputStream destination, boolean closeStream) throws Exception {

		try {
			Request request = new Request.Builder().url(
							url + ZuliaConstants.ASSOCIATED_DOCUMENTS_URL + "?" + HttpHelper.createQuery(createParameters(uniqueId, indexName, fileName)))
					.method("GET", null).build();
			Response response = client.newCall(request).execute();
			BufferedSink sink = Okio.buffer(Okio.sink(destination));
			sink.writeAll(Objects.requireNonNull(response.body(), "No body for file '" + fileName + "'.").source());
			sink.close();
			response.close();
		}
		finally {
			if (closeStream) {
				destination.close();
			}
		}

	}

	public void fetchAssociatedMetadata(String uniqueId, String indexName, String fileName, OutputStream destination) {

		try {
			Request request = new Request.Builder().url(
							url + ZuliaConstants.ASSOCIATED_DOCUMENTS_URL + "?" + HttpHelper.createQuery(createParameters(uniqueId, indexName, fileName)))
					.method("GET", null).build();
			Response response = client.newCall(request).execute();
			Document document = ZuliaUtil.byteArrayToMongoDocument(Objects.requireNonNull(response.body()).bytes());
			destination.write(Objects.requireNonNull(document.toJson().getBytes(StandardCharsets.UTF_8), "No body for file"));
			response.close();
		}
		catch (Throwable t) {
			LOG.log(Level.SEVERE,
					"Failed to fetch metadata for file <" + fileName + "> for id <" + uniqueId + "> for index <" + indexName + ">: " + t.getMessage());
		}

	}

	public void fetchAssociated(String uniqueId, String indexName, OutputStream destination, boolean closeStream) throws Exception {

		Request request = new Request.Builder().url(
						url + ZuliaConstants.ASSOCIATED_DOCUMENTS_ALL_FOR_ID_URL + "?" + HttpHelper.createQuery(createParameters(uniqueId, indexName)))
				.method("GET", null).build();
		Response response = client.newCall(request).execute();

		String allIdsJson = Objects.requireNonNull(response.body()).string();
		JsonObject result = JsonParser.parseString(allIdsJson).getAsJsonObject();
		JsonArray filenames = result.getAsJsonArray("filenames");

		ZipOutputStream zipOutputStream = null;
		try {
			zipOutputStream = new ZipOutputStream(destination);

			for (int i = 0; i < filenames.size(); i++) {
				String filename = filenames.get(i).getAsString();
				String fileDir = filename + File.separator;
				zipOutputStream.putNextEntry(new ZipEntry(fileDir));
				zipOutputStream.putNextEntry(new ZipEntry(fileDir + filename));
				fetchAssociated(uniqueId, indexName, filename, zipOutputStream, false);
				zipOutputStream.putNextEntry(new ZipEntry(fileDir + filename + "_metadata.json"));
				fetchAssociatedMetadata(uniqueId, indexName, filename, zipOutputStream);
			}

		}
		finally {
			if (closeStream) {
				if (zipOutputStream != null) {
					zipOutputStream.close();
				}
			}
			response.close();
		}

	}

	private HashMap<String, Object> createParameters(String uniqueId, String indexName) {
		HashMap<String, Object> parameters = new HashMap<>();
		parameters.put(ZuliaConstants.ID, uniqueId);
		parameters.put(ZuliaConstants.INDEX, indexName);
		return parameters;
	}

	private HashMap<String, Object> createParameters(String uniqueId, String indexName, String fileName) {
		HashMap<String, Object> parameters = new HashMap<>();
		parameters.put(ZuliaConstants.ID, uniqueId);
		parameters.put(ZuliaConstants.FILE_NAME, fileName);
		parameters.put(ZuliaConstants.INDEX, indexName);
		return parameters;
	}

}
