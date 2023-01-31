package io.zulia.client.result;

import io.zulia.fields.GsonDocumentMapper;
import io.zulia.message.ZuliaServiceOuterClass.FetchResponse;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import static io.zulia.message.ZuliaServiceOuterClass.BatchFetchResponse;

public class BatchFetchResult extends Result {

    @SuppressWarnings("unused")
    private BatchFetchResponse batchFetchResponse;

    private Iterator<FetchResponse> fetchResults;

    public BatchFetchResult(Iterator<FetchResponse> fetchResults) {
        this.fetchResults = fetchResults;
    }

    public List<FetchResult> getFetchResults() {
        ArrayList<FetchResult> list = new ArrayList<>();
        getFetchResults(list::add);
        return list;
    }

    public void getFetchResults(Consumer<FetchResult> fetchResultHandler) {
        while (fetchResults.hasNext()) {
            FetchResult fetchResult = new FetchResult(fetchResults.next());
            fetchResultHandler.accept(fetchResult);
        }
    }

    public <T> List<T> getDocuments(GsonDocumentMapper<T> mapper) throws Exception {
        ArrayList<T> list = new ArrayList<>();
        getDocuments(mapper, list::add);
        return list;
    }

    public <T> void getDocuments(GsonDocumentMapper<T> mapper, Consumer<T> docHandler) throws Exception {

        while (fetchResults.hasNext()) {
            FetchResult fetchResult = new FetchResult(fetchResults.next());
            if (fetchResult.hasResultDocument()) {
                docHandler.accept(fetchResult.getDocument(mapper));
            }
        }

    }

}
