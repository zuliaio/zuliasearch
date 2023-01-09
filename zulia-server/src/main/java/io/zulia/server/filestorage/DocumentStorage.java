package io.zulia.server.filestorage;

import io.zulia.message.ZuliaBase.AssociatedDocument;
import io.zulia.message.ZuliaBase.ExternalDocument;
import io.zulia.message.ZuliaQuery.FetchType;
import org.bson.Document;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Writer;
import java.util.List;

public interface DocumentStorage {

	void storeAssociatedDocument(AssociatedDocument docs) throws Exception;

	List<AssociatedDocument> getAssociatedDocuments(String uniqueId, FetchType fetchType) throws Exception;

	AssociatedDocument getAssociatedDocument(String uniqueId, String filename, FetchType fetchType) throws Exception;

	void getAssociatedDocuments(Writer writer, Document filter) throws Exception;

	OutputStream getAssociatedDocumentOutputStream(String uniqueId, String fileName, long timestamp, Document metadataMap) throws Exception;

	InputStream getAssociatedDocumentStream(String uniqueId, String filename) throws Exception;

	List<String> getAssociatedFilenames(String uniqueId) throws Exception;

	void deleteAssociatedDocument(String uniqueId, String fileName) throws Exception;

	void deleteAssociatedDocuments(String uniqueId) throws Exception;

	/**
	 * The method accepts the registration of an associated document that was created externally.  Primary use cae being something like S3.
	 * Unsupported by local files storage
	 * @param registration
	 * @throws Exception
	 */
	void registerExternalDocument(ExternalDocument registration) throws Exception;

	void drop() throws Exception;

	void deleteAllDocuments() throws Exception;

}
