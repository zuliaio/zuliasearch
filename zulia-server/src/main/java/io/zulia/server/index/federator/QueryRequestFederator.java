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

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

public class QueryRequestFederator extends MasterSlaveNodeRequestFederator<QueryRequest, InternalQueryResponse> {
	private final InternalClient internalClient;
	private final Collection<ZuliaIndex> indexes;

	private final Map<String, Query> queryMap;

	private static final Logger LOG = Logger.getLogger(QueryRequestFederator.class.getSimpleName());

	private static AtomicLong queryNumber = new AtomicLong();

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
			IndexShardResponse isr = index.internalQuery(queryMap.get(index.getIndexName()), request);
			internalQueryResponseBuilder.addIndexShardResponse(isr);
		}
		return internalQueryResponseBuilder.build();
	}

	public QueryResponse getResponse(QueryRequest request) throws Exception {

		long queryId = queryNumber.getAndIncrement();

		long start = System.currentTimeMillis();
		try {

			String queryJson = JsonFormat.printer().print(request);
			LOG.info("Running id <" + queryId + "> query <" + queryJson + ">");

			List<InternalQueryResponse> results = send(request);

			QueryCombiner queryCombiner = new QueryCombiner(indexes, request, results);

			QueryResponse qr = queryCombiner.getQueryResponse();

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
		finally {
			long end = System.currentTimeMillis();
			LOG.info("Finished query id <" + queryId + "> in " + (end - start) + "ms");
		}
	}
}
