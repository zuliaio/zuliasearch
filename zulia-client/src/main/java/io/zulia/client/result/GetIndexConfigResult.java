package io.zulia.client.result;

import io.zulia.client.config.ClientIndexConfig;

/**
 * Created by Payam Meyer on 4/3/17.
 *
 * @author pmeyer
 */
public class GetIndexConfigResult extends Result {

    private ClientIndexConfig indexConfig;

    public GetIndexConfigResult(ClientIndexConfig indexConfig) {
        this.indexConfig = indexConfig;
    }

    public ClientIndexConfig getIndexConfig() {
        return indexConfig;
    }

    public void setIndexConfig(ClientIndexConfig indexConfig) {
        this.indexConfig = indexConfig;
    }
}
