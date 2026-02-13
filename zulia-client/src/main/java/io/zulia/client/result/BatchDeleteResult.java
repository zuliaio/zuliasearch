package io.zulia.client.result;

import io.zulia.message.ZuliaServiceOuterClass.DeleteResponse;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class BatchDeleteResult extends Result {

	private final List<DeleteResponse> deleteResponses;

	public BatchDeleteResult(Iterator<DeleteResponse> batchDeleteResponse) {
		this.deleteResponses = new ArrayList<>();
		while (batchDeleteResponse.hasNext()) {
			deleteResponses.add(batchDeleteResponse.next());
		}
	}

	public int getDeleteCount() {
		return deleteResponses.size();
	}

}
