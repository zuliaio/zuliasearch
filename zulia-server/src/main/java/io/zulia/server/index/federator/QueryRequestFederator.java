package io.zulia.server.index.federator;

import com.google.protobuf.util.JsonFormat;
import io.zulia.message.ZuliaBase.MasterSlaveSettings;
import io.zulia.message.ZuliaBase.Node;
import io.zulia.message.ZuliaQuery.IndexShardResponse;
import io.zulia.message.ZuliaServiceOuterClass.InternalQueryRequest;
import io.zulia.message.ZuliaServiceOuterClass.InternalQueryResponse;
import io.zulia.message.ZuliaServiceOuterClass.QueryRequest;
import io.zulia.message.ZuliaServiceOuterClass.QueryResponse;
import io.zulia.server.connection.client.InternalClient;
import io.zulia.server.index.ZuliaIndex;
import io.zulia.server.search.QueryCombiner;
import org.apache.lucene.search.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;

public class QueryRequestFederator extends MasterSlaveNodeRequestFederator<QueryRequest, InternalQueryResponse> {

	private static final Logger LOG = LoggerFactory.getLogger(QueryRequestFederator.class);
	private static final AtomicLong QUERY_NUMBER = new AtomicLong();
	private final InternalClient internalClient;
	private final Collection<ZuliaIndex> indexes;
	private final Map<String, Query> queryMap;

	public QueryRequestFederator(Node thisNode, Collection<Node> otherNodesActive, MasterSlaveSettings masterSlaveSettings, Collection<ZuliaIndex> indexes,
			ExecutorService pool, InternalClient internalClient, Map<String, Query> queryMap) throws IOException {
		super(thisNode, otherNodesActive, masterSlaveSettings, indexes, pool);
		this.internalClient = internalClient;
		this.indexes = indexes;
		this.queryMap = queryMap;
	}

	@Override
	protected InternalQueryResponse processExternal(Node node, QueryRequest request) throws Exception {
		InternalQueryRequest internalQueryRequest = InternalQueryRequest.newBuilder().addAllIndexRouting(getIndexRouting(node)).setQueryRequest(request)
				.build();
		return internalClient.executeQuery(node, internalQueryRequest);
	}

	@Override
	protected InternalQueryResponse processInternal(Node node, QueryRequest request) throws Exception {
		InternalQueryRequest internalQueryRequest = InternalQueryRequest.newBuilder().addAllIndexRouting(getIndexRouting(node)).setQueryRequest(request)
				.build();
		return internalQuery(indexes, internalQueryRequest, queryMap);
	}

	public static InternalQueryResponse internalQuery(Collection<ZuliaIndex> indexes, InternalQueryRequest request, Map<String, Query> queryMap)
			throws Exception {
		InternalQueryResponse.Builder internalQueryResponseBuilder = InternalQueryResponse.newBuilder();
		for (ZuliaIndex index : indexes) {
			Query query = queryMap.get(index.getIndexName());
			IndexShardResponse isr = index.internalQuery(query, request);
			internalQueryResponseBuilder.addIndexShardResponse(isr);
		}
		return internalQueryResponseBuilder.build();
	}

	public QueryResponse getResponse(QueryRequest request) throws Exception {

		long queryId = QUERY_NUMBER.getAndIncrement();

		long start = System.currentTimeMillis();

		String queryJson = JsonFormat.printer().print(request);

		String searchLabel = request.getSearchLabel();
		if (searchLabel.isEmpty()) {
			LOG.info("Running id <" + queryId + "> query <" + queryJson + ">");
		}
		else {
			LOG.info("Running id <" + queryId + "> with label <" + searchLabel + "> query <" + queryJson + ">");
		}

		List<InternalQueryResponse> results = send(request);

		QueryCombiner queryCombiner = new QueryCombiner(indexes, request, results);

		QueryResponse qr = queryCombiner.getQueryResponse();

		long end = System.currentTimeMillis();
		handleLog(queryId, searchLabel, qr, end - start);
		if (!queryCombiner.isShort()) {
			return qr;
		}
		else {
			if (!request.getFetchFull()) {
				QueryRequest newRequest = request.toBuilder().setFetchFull(true).build();
				return getResponse(newRequest);
			}
			throw new Exception("Full fetch request is short");
		}

	}

	private static void handleLog(long queryId, String searchLabel, QueryResponse qr, long time) {
		String prefix = "Finished query";
		if (qr.getShardsQueried() == qr.getShardsPinned()) {
			prefix = "Finished query from pinned cache";
		}
		else if (qr.getShardsQueried() == qr.getShardsCached()) {
			prefix = "Finished query from cache";
		}

		String resultSize = String.format("%.2f", (qr.getSerializedSize() / 1024.0));

		if (searchLabel.isEmpty()) {
			LOG.info(prefix + " id <" + queryId + "> with result size " + resultSize + "KB in " + time + "ms");
		}
		else {
			LOG.info(prefix + " id <" + queryId + "> with label <" + searchLabel + "> with result size " + resultSize + "KB in " + time + "ms");
		}
	}
}
