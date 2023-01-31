package io.zulia.cache;

import java.util.Objects;

public class DocId {
    private final String uniqueId;
    private final String indexName;

    public DocId(String uniqueId, String indexName) {
        this.uniqueId = uniqueId;
        this.indexName = indexName;
    }

    public String getUniqueId() {
        return uniqueId;
    }

    public String getIndexName() {
        return indexName;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((indexName == null) ? 0 : indexName.hashCode());
        result = prime * result + ((uniqueId == null) ? 0 : uniqueId.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        DocId docId = (DocId) o;
        return Objects.equals(uniqueId, docId.uniqueId) && Objects.equals(indexName, docId.indexName);
    }
}