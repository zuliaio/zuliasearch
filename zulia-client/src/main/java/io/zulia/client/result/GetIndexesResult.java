package io.zulia.client.result;

import java.util.List;

import static io.zulia.message.ZuliaServiceOuterClass.GetIndexesResponse;

public class GetIndexesResult extends Result {

	private final GetIndexesResponse getIndexesResponse;

	public GetIndexesResult(GetIndexesResponse getIndexesResponse) {
		this.getIndexesResponse = getIndexesResponse;
	}

	public List<String> getIndexNames() {
		return getIndexesResponse.getIndexNameList();
	}

	public boolean containsIndex(String indexName) {
		return getIndexesResponse.getIndexNameList().contains(indexName);
	}

	public int getIndexCount() {
		return getIndexesResponse.getIndexNameCount();
	}

	@Override
	public String toString() {
		return getIndexesResponse.toString();
	}
}
