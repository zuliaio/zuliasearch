package io.zulia.client;

import io.micronaut.http.HttpRequest;
import io.micronaut.http.MediaType;
import io.micronaut.http.client.HttpClient;
import io.micronaut.http.client.multipart.MultipartBody;
import io.zulia.ZuliaConstants;
import io.zulia.util.HttpHelper;
import io.zulia.util.StreamHelper;
import org.bson.Document;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

public class ZuliaRESTClient {
	private String server;
	private int restPort;

	public ZuliaRESTClient(String server) {
		this(server, ZuliaConstants.DEFAULT_REST_SERVICE_PORT);
	}

	public ZuliaRESTClient(String server, int restPort) {
		this.server = server;
		this.restPort = restPort;
	}

	public void fetchAssociated(String uniqueId, String indexName, String fileName, File outputFile) throws IOException {
		fetchAssociated(uniqueId, indexName, fileName, new FileOutputStream(outputFile));
	}

	public HttpURLConnection fetchAssociated(String uniqueId, String indexName, String fileName) throws IOException {

		HashMap<String, Object> parameters = createParameters(uniqueId, indexName, fileName);

		String url = HttpHelper.createRequestUrl(server, restPort, ZuliaConstants.ASSOCIATED_DOCUMENTS_URL, parameters);
		HttpURLConnection conn = createGetConnection(url);
		handlePossibleError(conn);
		return conn;

	}

	public void fetchAssociated(String uniqueId, String indexName, String fileName, OutputStream destination) throws IOException {
		InputStream source = null;
		HttpURLConnection conn = null;

		try {
			HashMap<String, Object> parameters = createParameters(uniqueId, indexName, fileName);

			String url = HttpHelper.createRequestUrl(server, restPort, ZuliaConstants.ASSOCIATED_DOCUMENTS_URL, parameters);
			conn = createGetConnection(url);

			handlePossibleError(conn);

			source = conn.getInputStream();
			StreamHelper.copyStream(source, destination);
		}
		finally {
			closeStreams(source, destination, conn);
		}
	}

	protected HashMap<String, Object> createParameters(String uniqueId, String indexName, String fileName) {
		HashMap<String, Object> parameters = new HashMap<>();
		parameters.put(ZuliaConstants.ID, uniqueId);
		parameters.put(ZuliaConstants.FILE_NAME, fileName);
		parameters.put(ZuliaConstants.INDEX, indexName);
		return parameters;
	}

	public void storeAssociated(String uniqueId, String indexName, String fileName, File fileToStore) throws Exception {
		storeAssociated(uniqueId, indexName, fileName, new FileInputStream(fileToStore));
	}

	public void storeAssociated(String uniqueId, String indexName, String fileName, InputStream source) throws Exception {
		storeAssociated(uniqueId, indexName, fileName, null, source);
	}

	public void storeAssociated(String uniqueId, String indexName, String fileName, Document metadata, InputStream source) throws Exception {

		String url = HttpHelper.createRequestUrl(server, restPort, ZuliaConstants.ASSOCIATED_DOCUMENTS_URL, null);
		try (HttpClient client = HttpClient.create(new URI(url).toURL())) {
			MultipartBody requestBody = MultipartBody.builder().addPart("id", uniqueId).addPart("index", indexName).addPart("fileName", fileName)
					.addPart("metaJson", metadata.toJson()).addPart("file", fileName, MediaType.forFilename(fileName), source, 0).build();
			client.toBlocking().exchange(HttpRequest.POST(url, requestBody).contentType(MediaType.MULTIPART_FORM_DATA), String.class);
		}

	}

	private void handlePossibleError(HttpURLConnection conn) throws IOException {
		if (conn.getResponseCode() != ZuliaConstants.SUCCESS) {
			byte[] bytes;
			if (conn.getErrorStream() != null) {
				bytes = StreamHelper.getBytesFromStream(conn.getErrorStream());
			}
			else {
				bytes = StreamHelper.getBytesFromStream(conn.getInputStream());
			}
			throw new IOException("Request failed with <" + conn.getResponseCode() + ">: " + new String(bytes, StandardCharsets.UTF_8));
		}
	}

	private void closeStreams(InputStream source, OutputStream destination, HttpURLConnection conn) {
		if (source != null) {
			try {
				source.close();
			}
			catch (Exception e) {

			}
		}

		if (destination != null) {
			try {
				destination.close();
			}
			catch (Exception e) {

			}
		}

		if (conn != null) {
			conn.disconnect();
		}
	}

	protected HttpURLConnection createGetConnection(String url) throws IOException {
		HttpURLConnection conn;
		conn = (HttpURLConnection) (new URL(url)).openConnection();
		conn.setDoInput(true);
		conn.setDoOutput(true);
		conn.setRequestMethod(ZuliaConstants.GET);
		conn.connect();
		return conn;
	}

}
