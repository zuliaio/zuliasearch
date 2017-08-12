package io.zulia.client.command;

import io.zulia.client.command.base.SimpleCommand;
import io.zulia.client.pool.ZuliaConnection;
import io.zulia.client.result.BatchFetchResult;
import io.zulia.client.result.QueryResult;
import io.zulia.message.ZuliaQuery;
import io.zulia.message.ZuliaServiceGrpc.ZuliaServiceBlockingStub;
import io.zulia.message.ZuliaServiceOuterClass.BatchFetchRequest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static io.zulia.message.ZuliaQuery.FetchType;
import static io.zulia.message.ZuliaQuery.ScoredResult;
import static io.zulia.message.ZuliaServiceOuterClass.BatchFetchResponse;

/**
 * Fetches multiple documents in a single call
 * @author mdavis
 *
 */
public class BatchFetch extends SimpleCommand<BatchFetchRequest, BatchFetchResult> {

	private List<Fetch> fetchList;

	public BatchFetch() {
		this.fetchList = new ArrayList<>();
	}

	public BatchFetch addFetches(Collection<? extends Fetch> fetches) {
		this.fetchList.addAll(fetches);
		return this;
	}

	public BatchFetch addFetchDocumentsFromUniqueIds(Collection<String> uniqueIds, String indexName) {

		for (String uniqueId : uniqueIds) {
			Fetch f = new Fetch(uniqueId, indexName);
			f.setResultFetchType(FetchType.FULL);
			f.setAssociatedFetchType(FetchType.NONE);
			fetchList.add(f);
		}
		return this;
	}

	public BatchFetch addFetchDocumentsFromResults(QueryResult qr) {
		return addFetchDocumentsFromResults(qr.getResults());
	}

	public BatchFetch addFetchDocumentsFromResults(Collection<ZuliaQuery.ScoredResult> scoredResults) {

		for (ScoredResult scoredResult : scoredResults) {
			Fetch f = new Fetch(scoredResult.getUniqueId(), scoredResult.getIndexName());
			f.setResultFetchType(FetchType.FULL);
			f.setAssociatedFetchType(FetchType.NONE);
			fetchList.add(f);
		}
		return this;
	}

	@Override
	public BatchFetchRequest getRequest() {
		BatchFetchRequest.Builder batchFetchRequestBuilder = BatchFetchRequest.newBuilder();
		for (Fetch f : fetchList) {
			batchFetchRequestBuilder.addFetchRequest(f.getRequest());
		}
		return batchFetchRequestBuilder.build();
	}

	@Override
	public BatchFetchResult execute(ZuliaConnection zuliaConnection) {
		ZuliaServiceBlockingStub service = zuliaConnection.getService();

		BatchFetchResponse batchFetchResponse = service.batchFetch(getRequest());

		return new BatchFetchResult(batchFetchResponse);
	}

}
