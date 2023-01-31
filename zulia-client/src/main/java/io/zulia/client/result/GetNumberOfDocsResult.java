package io.zulia.client.result;

import io.zulia.message.ZuliaServiceOuterClass.GetNumberOfDocsResponse;

import java.util.List;

import static io.zulia.message.ZuliaBase.ShardCountResponse;

public class GetNumberOfDocsResult extends Result {

    private GetNumberOfDocsResponse getNumberOfDocsResponse;

    public GetNumberOfDocsResult(GetNumberOfDocsResponse getNumberOfDocsResponse) {
        this.getNumberOfDocsResponse = getNumberOfDocsResponse;
    }

    public long getNumberOfDocs() {
        return getNumberOfDocsResponse.getNumberOfDocs();
    }

    public int getShardCountResponseCount() {
        return getNumberOfDocsResponse.getShardCountResponseCount();
    }

    public List<ShardCountResponse> getShardCountResponses() {
        return getNumberOfDocsResponse.getShardCountResponseList();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();

        sb.append("{\n  \"numberOfDocs\": ");
        sb.append(getNumberOfDocs());
        sb.append(",\n  \"segmentCounts\": [");
        for (ShardCountResponse scr : getShardCountResponses()) {
            sb.append("\n    {\n      \"segmentNumber\": ");
            sb.append(scr.getShardNumber());
            sb.append(",\n      \"numberOfDocs\": ");
            sb.append(scr.getNumberOfDocs());
            sb.append("\n    },");
        }
        if (getShardCountResponseCount() != 0) {
            sb.setLength(sb.length() - 1);
        }
        sb.append("\n  ],\n  \"commandTimeMs\": ");
        sb.append(getCommandTimeMs());
        sb.append("\n}\n");
        return sb.toString();
    }

}
