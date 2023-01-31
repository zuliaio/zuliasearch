package io.zulia.client.result;

import io.zulia.message.ZuliaBase;

import java.util.List;

import static io.zulia.message.ZuliaServiceOuterClass.GetTermsResponse;

public class GetTermsResult extends Result {

    private GetTermsResponse getTermsResponse;

    public GetTermsResult(GetTermsResponse getTermsResponse, long commandTimeMs) {
        this.getTermsResponse = getTermsResponse;
    }

    public List<ZuliaBase.Term> getTerms() {
        return getTermsResponse.getTermList();
    }

    public ZuliaBase.Term getLastTerm() {
        if (getTermsResponse.hasLastTerm()) {
            return getTermsResponse.getLastTerm();
        }
        return null;
    }

    @Override
    public String toString() {
        return getTermsResponse.toString();
    }

}
