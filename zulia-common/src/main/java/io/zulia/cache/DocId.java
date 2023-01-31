package io.zulia.cache;

import java.util.Objects;

public record DocId(String uniqueId, String indexName) {


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