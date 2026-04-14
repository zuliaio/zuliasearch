package io.zulia.ai.embedding;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

public class HuggingFaceModelDownloader {

	private static final String HF_BASE = "https://huggingface.co/";
	private static final String[] ONNX_PATHS = { "onnx/model.onnx", "model.onnx" };

	private static final Path CACHE_DIR = resolveCacheDir();

	private static Path resolveCacheDir() {
		String djlCache = System.getenv("DJL_CACHE_DIR");
		if (djlCache == null) {
			djlCache = System.getProperty("DJL_CACHE_DIR");
		}
		if (djlCache != null) {
			return Path.of(djlCache, "huggingface");
		}
		return Path.of(System.getProperty("user.home"), ".djl.ai", "huggingface");
	}

	public static Path downloadModel(String modelUrl) throws IOException {
		String modelId = extractModelId(modelUrl);
		Path modelDir = CACHE_DIR.resolve(modelId.replace("/", "_"));
		Path onnxFile = modelDir.resolve("model.onnx");
		Path tokenizerFile = modelDir.resolve("tokenizer.json");

		if (Files.exists(onnxFile) && Files.exists(tokenizerFile)) {
			return modelDir;
		}

		Files.createDirectories(modelDir);

		if (!Files.exists(onnxFile)) {
			downloadOnnxModel(modelId, onnxFile);
		}

		if (!Files.exists(tokenizerFile)) {
			downloadFile(HF_BASE + modelId + "/resolve/main/tokenizer.json", tokenizerFile);
		}

		return modelDir;
	}

	private static void downloadOnnxModel(String modelId, Path target) throws IOException {
		for (String onnxPath : ONNX_PATHS) {
			String url = HF_BASE + modelId + "/resolve/main/" + onnxPath;
			try {
				downloadFile(url, target);
				return;
			}
			catch (IOException e) {
				// try next path
			}
		}
		throw new IOException("No ONNX model found for " + modelId + " (tried: " + String.join(", ", ONNX_PATHS) + ")");
	}

	private static void downloadFile(String url, Path target) throws IOException {
		try (HttpClient client = HttpClient.newBuilder().followRedirects(HttpClient.Redirect.ALWAYS).build()) {
			HttpRequest request = HttpRequest.newBuilder().uri(URI.create(url)).GET().build();
			HttpResponse<InputStream> response = client.send(request, HttpResponse.BodyHandlers.ofInputStream());
			if (response.statusCode() != 200) {
				throw new IOException("HTTP " + response.statusCode() + " downloading " + url);
			}
			Path temp = target.resolveSibling(target.getFileName() + ".tmp");
			try (InputStream is = response.body()) {
				Files.copy(is, temp, StandardCopyOption.REPLACE_EXISTING);
			}
			Files.move(temp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
		}
		catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IOException("Download interrupted: " + url, e);
		}
	}

	private static String extractModelId(String url) {
		if (url.startsWith(HF_BASE)) {
			String path = url.substring(HF_BASE.length());
			String[] parts = path.split("/");
			if (parts.length >= 2) {
				return parts[0] + "/" + parts[1];
			}
		}
		return url;
	}
}
