package io.zulia.doc;

import com.google.protobuf.ByteString;
import io.zulia.message.ZuliaBase.AssociatedDocument;
import io.zulia.util.ZuliaUtil;
import org.bson.Document;

public class AssociatedBuilder {

    private AssociatedDocument.Builder adBuilder;

    public static AssociatedBuilder newBuilder() {
        return new AssociatedBuilder();
    }

    public AssociatedBuilder() {
        adBuilder = AssociatedDocument.newBuilder();
    }

    public AssociatedBuilder setFilename(String filename) {
        adBuilder.setFilename(filename);
        return this;
    }

    public AssociatedBuilder setDocumentUniqueId(String documentUniqueId) {
        adBuilder.setDocumentUniqueId(documentUniqueId);
        return this;
    }

    public AssociatedBuilder setIndexName(String indexName) {
        adBuilder.setIndexName(indexName);
        return this;
    }

    public AssociatedBuilder setDocument(String utf8Text) {
        adBuilder.setDocument(ByteString.copyFromUtf8(utf8Text));
        return this;
    }

    public AssociatedBuilder setDocument(byte[] bytes) {
        adBuilder.setDocument(ByteString.copyFrom(bytes));
        return this;
    }

    public AssociatedBuilder setDocument(Document document) {
        adBuilder.setDocument(ZuliaUtil.mongoDocumentToByteString(document));
        return this;
    }

    public AssociatedBuilder setMetadata(Document metadata) {
        if (metadata != null) {
            adBuilder.setMetadata(ZuliaUtil.mongoDocumentToByteString(metadata));
        } else {
            adBuilder.clearMetadata();
        }
        return this;
    }

    public AssociatedBuilder clear() {
        adBuilder.clear();
        return this;
    }

    public AssociatedDocument getAssociatedDocument() {
        return adBuilder.build();
    }

}
