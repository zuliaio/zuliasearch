package io.zulia.server.index.federator;

import io.zulia.message.ZuliaBase;
import io.zulia.message.ZuliaBase.Node;
import io.zulia.message.ZuliaBase.ShardCountResponse;
import io.zulia.message.ZuliaServiceOuterClass;
import io.zulia.message.ZuliaServiceOuterClass.GetNumberOfDocsRequest;
import io.zulia.message.ZuliaServiceOuterClass.GetNumberOfDocsResponse;
import io.zulia.message.ZuliaServiceOuterClass.InternalGetNumberOfDocsRequest;
import io.zulia.server.connection.client.InternalClient;
import io.zulia.server.index.ZuliaIndex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class GetNumberOfDocsRequestFederator extends MasterSlaveNodeRequestFederator<GetNumberOfDocsRequest, GetNumberOfDocsResponse> {
    private final InternalClient internalClient;
    private final ZuliaIndex index;

    public GetNumberOfDocsRequestFederator(Node thisNode, Collection<Node> otherNodesActive, ZuliaBase.MasterSlaveSettings masterSlaveSettings,
                                           ZuliaIndex index, ExecutorService pool, InternalClient internalClient) throws IOException {
        super(thisNode, otherNodesActive, masterSlaveSettings, index, pool);
        this.internalClient = internalClient;
        this.index = index;
    }

    @Override
    protected GetNumberOfDocsResponse processExternal(Node node, GetNumberOfDocsRequest request) throws Exception {
        ZuliaServiceOuterClass.IndexRouting indexRouting = getIndexRouting(node).get(0);
        InternalGetNumberOfDocsRequest internalRequest = InternalGetNumberOfDocsRequest.newBuilder().setIndexRouting(indexRouting)
                .setGetNumberOfDocsRequest(request).build();
        return internalClient.getNumberOfDocs(node, internalRequest);
    }

    @Override
    protected GetNumberOfDocsResponse processInternal(Node node, GetNumberOfDocsRequest request) throws Exception {
        ZuliaServiceOuterClass.IndexRouting indexRouting = getIndexRouting(node).get(0);
        InternalGetNumberOfDocsRequest internalRequest = InternalGetNumberOfDocsRequest.newBuilder().setIndexRouting(indexRouting)
                .setGetNumberOfDocsRequest(request).build();
        return internalGetNumberOfDocs(index, internalRequest);
    }

    public static GetNumberOfDocsResponse internalGetNumberOfDocs(ZuliaIndex index, InternalGetNumberOfDocsRequest request) throws Exception {
        return index.getNumberOfDocs(request);
    }

    public GetNumberOfDocsResponse getResponse(GetNumberOfDocsRequest request) throws Exception {

        List<GetNumberOfDocsResponse> responses = send(request);

        GetNumberOfDocsResponse.Builder responseBuilder = GetNumberOfDocsResponse.newBuilder();
        responseBuilder.setNumberOfDocs(0);

        List<ShardCountResponse> shardCountResponseList = new ArrayList<>();

        for (GetNumberOfDocsResponse r : responses) {
            responseBuilder.setNumberOfDocs(responseBuilder.getNumberOfDocs() + r.getNumberOfDocs());
            shardCountResponseList.addAll(r.getShardCountResponseList());
        }

        shardCountResponseList.sort(Comparator.comparingInt(ShardCountResponse::getShardNumber));
        responseBuilder.addAllShardCountResponse(shardCountResponseList);

        return responseBuilder.build();

    }
}
