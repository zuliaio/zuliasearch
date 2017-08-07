package io.zulia.client.command;

import io.zulia.client.config.IndexConfig;
import io.zulia.client.result.Result;

/**
 * Created by Payam Meyer on 4/3/17.
 * @author pmeyer
 */
public class GetIndexConfigResult extends Result {

	private IndexConfig indexConfig;

	public GetIndexConfigResult(IndexConfig indexConfig) {
		this.indexConfig = indexConfig;
	}

	public IndexConfig getIndexConfig() {
		return indexConfig;
	}

	public void setIndexConfig(IndexConfig indexConfig) {
		this.indexConfig = indexConfig;
	}
}
