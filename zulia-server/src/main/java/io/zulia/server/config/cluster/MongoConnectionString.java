package io.zulia.server.config.cluster;

public class MongoConnectionString {
    private String protocol = "mongodb+srv://";
    private String connectionURL;
    private boolean retryWrites = true;
    private String writeConcern = "primary";

    public String getConnectionURL() {
        return connectionURL;
    }

    public void setConnectionURL(String connectionURL) {
        this.connectionURL = connectionURL;
    }

    public boolean isRetryWrites() {
        return retryWrites;
    }

    public void setRetryWrites(boolean retryWrites) {
        this.retryWrites = retryWrites;
    }

    public String getWriteConcern() {
        return writeConcern;
    }

    public void setWriteConcern(String writeConcern) {
        this.writeConcern = writeConcern;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }
}
