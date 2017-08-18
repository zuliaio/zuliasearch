package io.zulia.client.command;

import io.zulia.client.command.base.SimpleCommand;
import io.zulia.client.pool.ZuliaConnection;
import io.zulia.client.result.BatchDeleteResult;
import io.zulia.client.result.QueryResult;
import io.zulia.message.ZuliaServiceOuterClass;
import io.zulia.message.ZuliaServiceOuterClass.DeleteResponse;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static io.zulia.message.ZuliaQuery.ScoredResult;
import static io.zulia.message.ZuliaServiceGrpc.ZuliaServiceBlockingStub;
import static io.zulia.message.ZuliaServiceOuterClass.BatchDeleteRequest;

public class BatchDelete extends SimpleCommand<ZuliaServiceOuterClass.BatchDeleteRequest, BatchDeleteResult> {

	private List<Delete> deletes;

	public BatchDelete() {
		deletes = new ArrayList<Delete>();
	}

	public BatchDelete addDelete(Delete delete) {
		deletes.add(delete);
		return this;
	}

	public BatchDelete deleteDocumentFromQueryResult(QueryResult queryResult) {

		for (ScoredResult sr : queryResult.getResults()) {
			Delete delete = new DeleteDocument(sr.getUniqueId(), sr.getIndexName());
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
