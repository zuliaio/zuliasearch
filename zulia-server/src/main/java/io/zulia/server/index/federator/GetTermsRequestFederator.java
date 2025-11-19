package io.zulia.server.index.federator;

import io.zulia.message.ZuliaBase;
import io.zulia.message.ZuliaBase.MasterSlaveSettings;
import io.zulia.message.ZuliaBase.Node;
import io.zulia.message.ZuliaBase.Term;
import io.zulia.message.ZuliaServiceOuterClass.GetTermsRequest;
import io.zulia.message.ZuliaServiceOuterClass.GetTermsResponse;
import io.zulia.message.ZuliaServiceOuterClass.IndexRouting;
import io.zulia.message.ZuliaServiceOuterClass.InternalGetTermsRequest;
import io.zulia.message.ZuliaServiceOuterClass.InternalGetTermsResponse;
import io.zulia.server.connection.client.InternalClient;
import io.zulia.server.index.ZuliaIndex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public class GetTermsRequestFederator extends MasterSlaveNodeRequestFederator<GetTermsRequest, InternalGetTermsResponse> {
	private final InternalClient internalClient;
	private final ZuliaIndex index;

	public GetTermsRequestFederator(Node thisNode, Collection<Node> otherNodesActive, MasterSlaveSettings masterSlaveSettings, ZuliaIndex index,
			ExecutorService pool, InternalClient internalClient) throws IOException {
		super(thisNode, otherNodesActive, masterSlaveSettings, index, pool);
		this.internalClient = internalClient;
		this.index = index;
	}

	@Override
	protected InternalGetTermsResponse processExternal(Node node, GetTermsRequest request) throws Exception {
		IndexRouting indexRouting = getIndexRouting(node).get(0);
		InternalGetTermsRequest internalRequest = InternalGetTermsRequest.newBuilder().setIndexRouting(indexRouting).setGetTermsRequest(request).build();
		return internalClient.getTerms(node, internalRequest);
	}

	@Override
	protected InternalGetTermsResponse processInternal(Node node, GetTermsRequest request) throws Exception {
		IndexRouting indexRouting = getIndexRouting(node).get(0);
		InternalGetTermsRequest internalRequest = InternalGetTermsRequest.newBuilder().setIndexRouting(indexRouting).setGetTermsRequest(request).build();
		return internalGetTerms(index, internalRequest);
	}

	public static InternalGetTermsResponse internalGetTerms(ZuliaIndex index, InternalGetTermsRequest request) throws Exception {
		return index.getTerms(request);
	}

	public GetTermsResponse getResponse(GetTermsRequest request) throws Exception {

		List<InternalGetTermsResponse> responses = send(request);

		Map<String, Term.Builder> terms = new HashMap<>();
		for (InternalGetTermsResponse response : responses) {
			for (GetTermsResponse gtr : response.getGetTermsResponseList()) {
				for (Term term : gtr.getTermList()) {
					String key = term.getValue();

					ZuliaBase.Term.Builder builder = terms.get(key);
					if (builder == null) {
						builder = Term.newBuilder().setValue(key).setDocFreq(0).setTermFreq(0).setScore(0);
						terms.put(key, builder);
					}
					builder.setDocFreq(builder.getDocFreq() + term.getDocFreq());
					builder.setTermFreq(builder.getTermFreq() + term.getTermFreq());
					builder.setScore(builder.getScore() + term.getScore());
				}
			}
		}

		GetTermsResponse.Builder responseBuilder = GetTermsResponse.newBuilder();

		Term.Builder value = null;

		int count = 0;

		int amount = request.getAmount();

		ArrayList<Map.Entry<String, Term.Builder>> entries = new ArrayList<>(terms.entrySet());
		entries.sort(Map.Entry.comparingByKey());

		for (Map.Entry<String, Term.Builder> entry : entries) {
			value = entry.getValue();
			if (value.getDocFreq() >= request.getMinDocFreq() && value.getTermFreq() >= request.getMinTermFreq()) {
				responseBuilder.addTerm(value.build());
				count++;
			}

			if (amount != 0 && count >= amount) {
				break;
			}
		}

		if (value != null) {
			responseBuilder.setLastTerm(value.build());
		}

		return responseBuilder.build();
	}
}
