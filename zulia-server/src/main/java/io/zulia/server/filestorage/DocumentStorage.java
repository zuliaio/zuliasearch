package io.zulia.server.filestorage;

import io.zulia.message.ZuliaBase.AssociatedDocument;
import io.zulia.message.ZuliaBase.ExternalDocument;
import io.zulia.message.ZuliaQuery.FetchType;
import io.zulia.rest.dto.AssociatedMetadataDTO;
import org.bson.Document;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.stream.Stream;

public interface DocumentStorage {

	String DOCUMENT_UNIQUE_ID_KEY = "_uid_";
	String FILE_UNIQUE_ID_KEY = "_fid_";

	void storeAssociatedDocument(AssociatedDocument docs) throws Exception;

	List<AssociatedDocument> getAssociatedMetadataForUniqueId(String uniqueId, FetchType fetchType) throws Exception;

	AssociatedDocument getAssociatedDocument(String uniqueId, String filename, FetchType fetchType) throws Exception;

	Stream<AssociatedMetadataDTO> getAssociatedMetadataForQuery(Document query);

	OutputStream getAssociatedDocumentOutputStream(String uniqueId, String fileName, long timestamp, Document metadataMap) throws Exception;

	InputStream getAssociatedDocumentStream(String uniqueId, String filename) throws Exception;

	List<String> getAssociatedFilenames(String uniqueId) throws Exception;

	void deleteAssociatedDocument(String uniqueId, String fileName) throws Exception;

	void deleteAssociatedDocuments(String uniqueId) throws Exception;

	/**
	 * The method accepts the registration of an associated document that was created externally.  Primary use cae being something like S3.
	 * Unsupported by local files storage
	 *
	 * @param registration
	 */
	void registerExternalDocument(ExternalDocument registration);

	void drop() throws Exception;

	void deleteAllDocuments() throws Exception;

}
