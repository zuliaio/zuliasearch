package io.zulia.client.rest;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.micronaut.http.HttpRequest;
import io.micronaut.http.HttpResponse;
import io.micronaut.http.MediaType;
import io.micronaut.http.client.multipart.MultipartBody;
import io.zulia.ZuliaConstants;
import io.zulia.util.HttpHelper;
import io.zulia.util.ZuliaUtil;
import org.bson.Document;
import reactor.core.publisher.Flux;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static io.micronaut.http.HttpRequest.GET;

public class ZuliaRESTClient {

	private static final Logger LOG = Logger.getLogger(ZuliaRESTClient.class.getName());
	private final String url;
	private MicronautHttpClient client;

	public ZuliaRESTClient(String server, int restPort) {
		url = "http://" + server + ":" + restPort;

		client = MicronautHttpClient.createClient(url);

	}

	public void storeAssociated(String uniqueId, String indexName, String fileName, Document metadata, byte[] bytes) throws Exception {

		MultipartBody body;
		if (metadata != null) {
			body = MultipartBody.builder().addPart("id", uniqueId).addPart("indexName", indexName).addPart("fileName", fileName)
					.addPart("metaJson", metadata.toJson()).addPart("file", fileName, MediaType.forFilename(fileName), bytes).build();
		}
		else {
			body = MultipartBody.builder().addPart("id", uniqueId).addPart("indexName", indexName).addPart("fileName", fileName)
					.addPart("file", fileName, MediaType.forFilename(fileName), bytes).build();
		}

		try {
			Flux<HttpResponse<String>> post = Flux.from(client.exchange(
					HttpRequest.POST(ZuliaConstants.ASSOCIATED_DOCUMENTS_URL, body).contentType(MediaType.MULTIPART_FORM_DATA).accept(MediaType.TEXT_PLAIN),
					String.class));
			post.blockFirst();
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

			Flux<HttpResponse<byte[]>> data = Flux.from(client.exchange(
					GET(ZuliaConstants.ASSOCIATED_DOCUMENTS_URL + "?" + HttpHelper.createQuery(createParameters(uniqueId, indexName, fileName))),
					byte[].class));

			data.doOnNext(httpResponse -> {
				try {
					destination.write(Objects.requireNonNull(httpResponse.body(), "No body for file"));
				}
				catch (IOException e) {
					throw new RuntimeException("Failed to fetch <" + fileName + "> for id <" + uniqueId + "> for index <" + indexName + ">: " + e.getMessage());
				}
			}).blockLast();

		}
		finally {
			if (closeStream) {
				destination.close();
			}
		}
	}

	public void fetchAssociatedMetadata(String uniqueId, String indexName, String fileName, OutputStream destination) {

		try {

			Flux<HttpResponse<byte[]>> data = Flux.from(client.exchange(
					GET(ZuliaConstants.ASSOCIATED_DOCUMENTS_METADATA_URL + "?" + HttpHelper.createQuery(createParameters(uniqueId, indexName, fileName))),
					byte[].class));

			data.doOnNext(httpResponse -> {
				try {
					Document document = ZuliaUtil.byteArrayToMongoDocument(httpResponse.body());
					destination.write(Objects.requireNonNull(document.toJson().getBytes(StandardCharsets.UTF_8), "No body for file"));
				}
				catch (IOException e) {
					throw new RuntimeException(
							"Failed to fetch metadata for file <" + fileName + "> for id <" + uniqueId + "> for index <" + indexName + ">: " + e.getMessage());
				}
			}).blockLast();

		}
		catch (Throwable throwable) {
			LOG.log(Level.SEVERE,
					"Failed to fetch metadata for file <" + fileName + "> for id <" + uniqueId + "> for index <" + indexName + ">: " + throwable.getMessage());
		}

	}

	public void fetchAssociated(String uniqueId, String indexName, OutputStream destination, boolean closeStream) throws Exception {

		String allIdsJson = client.toBlocking()
				.retrieve(GET(ZuliaConstants.ASSOCIATED_DOCUMENTS_ALL_FOR_ID_URL + "?" + HttpHelper.createQuery(createParameters(uniqueId, indexName))));
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

	public void close() {
		LOG.info("Closing REST client pool to " + url);
		client.close();
	}

}
