package io.zulia.client.command;

import io.zulia.client.command.base.SimpleCommand;
import io.zulia.client.pool.ZuliaConnection;
import io.zulia.client.result.BatchDeleteResult;
import io.zulia.client.result.CompleteResult;
import io.zulia.client.result.SearchResult;
import io.zulia.message.ZuliaServiceOuterClass;
import io.zulia.message.ZuliaServiceOuterClass.DeleteResponse;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static io.zulia.message.ZuliaServiceGrpc.ZuliaServiceBlockingStub;
import static io.zulia.message.ZuliaServiceOuterClass.BatchDeleteRequest;

public class BatchDelete extends SimpleCommand<ZuliaServiceOuterClass.BatchDeleteRequest, BatchDeleteResult> {

	private List<Delete> deletes;

	public BatchDelete() {
		deletes = new ArrayList<>();
	}

	public BatchDelete addDelete(Delete delete) {
		deletes.add(delete);
		return this;
	}

	public BatchDelete deleteDocumentFromQueryResult(SearchResult queryResult) {

		for (CompleteResult completeResult : queryResult.getCompleteResults()) {
			Delete delete = new DeleteDocument(completeResult.getUniqueId(), completeResult.getIndexName());
			deletes.add(delete);
		}

		return this;
	}

	@Override
	public BatchDeleteRequest getRequest() {
		BatchDeleteRequest.Builder batchDeleteRequest = BatchDeleteRequest.newBuilder();

		for (Delete delete : deletes) {
			batchDeleteRequest.addRequest(delete.getRequest());
		}

		return batchDeleteRequest.build();
	}

	@Override
	public BatchDeleteResult execute(ZuliaConnection zuliaConnection) {
		ZuliaServiceBlockingStub service = zuliaConnection.getService();

		Iterator<DeleteResponse> batchDeleteResponse = service.batchDelete(getRequest());

		return new BatchDeleteResult(batchDeleteResponse);
	}

}
