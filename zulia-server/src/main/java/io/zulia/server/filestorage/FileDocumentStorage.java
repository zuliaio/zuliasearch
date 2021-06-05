package io.zulia.server.filestorage;

import io.zulia.message.ZuliaBase.AssociatedDocument;
import io.zulia.message.ZuliaQuery.FetchType;
import io.zulia.server.config.ZuliaConfig;
import org.bson.Document;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Writer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.logging.Logger;

public class FileDocumentStorage implements DocumentStorage {
	@SuppressWarnings("unused")
	private final static Logger log = Logger.getLogger(FileDocumentStorage.class.getSimpleName());
	private final String indexName;
	private final String filesPath;

	public FileDocumentStorage(ZuliaConfig zuliaConfig, String indexName) {
		this.indexName = indexName;
		this.filesPath = zuliaConfig.getDataPath() + File.separator + "files";
	}

	@Override
	public void storeAssociatedDocument(AssociatedDocument docs) throws Exception {
		String pathToDocument = createPathIfNotExists(docs.getDocumentUniqueId(), docs.getFilename());
		Files.write(Path.of(pathToDocument, docs.getFilename()), docs.getDocument().toByteArray());
		//TODO document metadata / timestamp
	}

	@Override
	public void storeAssociatedDocument(String uniqueId, String fileName, InputStream is, long timestamp, Document metadataMap) throws Exception {
		String pathToDocument = createPathIfNotExists(uniqueId, fileName);
		throw new RuntimeException("Not implemented");
	}

	@Override
	public List<AssociatedDocument> getAssociatedDocuments(String uniqueId, FetchType fetchType) throws Exception {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public AssociatedDocument getAssociatedDocument(String uniqueId, String filename, FetchType fetchType) throws Exception {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public void getAssociatedDocuments(Writer writer, Document filter) throws IOException {
		throw new RuntimeException("Not implemented");
	}


	@Override
	public InputStream getAssociatedDocumentStream(String uniqueId, String filename) {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public List<String> getAssociatedFilenames(String uniqueId) throws Exception {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public void deleteAssociatedDocument(String uniqueId, String fileName) {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public void deleteAssociatedDocuments(String uniqueId) {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public void drop() {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public void deleteAllDocuments() {
		throw new RuntimeException("Not implemented");
	}

	private String createPathIfNotExists(String uniqueId, String fileName) throws Exception {
		String pathToDocument = getFullPathToDocument(uniqueId);
		File f = new File(pathToDocument);
		if (!f.exists()) {
			boolean created = f.mkdirs();
			if (!created) {
				if (!f.exists()) {
					throw new Exception(
							"Failed to create directory for associated document <" + fileName + "> for unique id <" + uniqueId + "> for indexName <" + indexName
									+ "> in path <" + pathToDocument + ">");
				}
				if (!f.isDirectory()) {
					throw new Exception(
							"Failed to create directory for associated document <" + fileName + "> for unique id <" + uniqueId + "> for indexName <" + indexName
									+ "> in path <" + pathToDocument + "> because path exist and is not a directory");
				}
			}
		}
		return pathToDocument;
	}

	private String getFullPathToDocument(String uniqueId) {
		return filesPath + File.separator + indexName + File.separator + getPathToDocument(uniqueId);
	}

	private static String getPathToDocument(String uniqueId) {
		String hexHash = Integer.toHexString(uniqueId.hashCode());
		String piece1 = hexHash.substring(0, 2);
		String piece2 = hexHash.substring(2, 5);
		String piece3 = hexHash.substring(5);
		return piece1 + File.separator + piece2 + File.separator + piece3;
	}

}
