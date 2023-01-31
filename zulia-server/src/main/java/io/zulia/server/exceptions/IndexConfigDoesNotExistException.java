package io.zulia.server.exceptions;

import java.io.IOException;

public class IndexConfigDoesNotExistException extends IOException {

    private static final long serialVersionUID = 1L;
    private String indexName;

    public IndexConfigDoesNotExistException(String name) {
        super("Index config <" + name + "> does not exist");
        this.indexName = name;
    }

    public String getIndexName() {
        return indexName;
    }

}
