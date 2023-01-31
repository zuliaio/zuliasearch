package io.zulia.client.command;

/**
 * Deletes a document from the Zulia index specified without
 * removing associated documents
 *
 * @author mdavis
 */
public class DeleteFromIndex extends Delete {

    public DeleteFromIndex(String uniqueId, String indexName) {
        super(uniqueId, indexName);
        setDeleteDocument(true);
        setDeleteAllAssociated(false);
    }

}
