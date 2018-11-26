package io.zulia.server.filestorage;

import io.zulia.message.ZuliaBase.AssociatedDocument;
import io.zulia.message.ZuliaQuery.FetchType;
import io.zulia.server.config.ZuliaConfig;
import org.bson.Document;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.logging.Logger;

public class FileDocumentStorage implements DocumentStorage {
	@SuppressWarnings("unused")
	private final static Logger log = Logger.getLogger(FileDocumentStorage.class.getSimpleName());

	public FileDocumentStorage(ZuliaConfig zuliaConfig, String indexName) {

	}

	@Override
	public void storeAssociatedDocument(AssociatedDocument docs) throws Exception {
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
	public void getAssociatedDocuments(OutputStream outputstream, Document filter) throws IOException {
		throw new RuntimeException("Not implemented");
	}

	@Override
	public void storeAssociatedDocument(String uniqueId, String fileName, InputStream is, long timestamp, Document metadataMap) throws Exception {
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
}
