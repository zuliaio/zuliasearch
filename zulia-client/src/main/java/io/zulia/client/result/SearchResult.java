package io.zulia.client.result;

import io.zulia.fields.GsonDocumentMapper;
import io.zulia.message.ZuliaQuery;
import io.zulia.message.ZuliaQuery.AnalysisResult;
import io.zulia.message.ZuliaQuery.FacetCount;
import io.zulia.message.ZuliaQuery.FacetGroup;
import io.zulia.message.ZuliaQuery.FacetStats;
import io.zulia.message.ZuliaQuery.LastResult;
import io.zulia.message.ZuliaQuery.ScoredResult;
import io.zulia.message.ZuliaQuery.StatGroup;
import io.zulia.message.ZuliaServiceOuterClass.QueryResponse;
import io.zulia.util.ResultHelper;
import io.zulia.util.ZuliaUtil;
import org.bson.Document;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class SearchResult extends Result {
	private final QueryResponse queryResponse;

	public SearchResult(QueryResponse queryResponse) {
		this.queryResponse = queryResponse;
	}

	public long getTotalHits() {
		return queryResponse.getTotalHits();
	}

	public boolean hasResults() {
		return !queryResponse.getResultsList().isEmpty();
	}

	/**
	 * use getCompleteResults instead
	 *
	 * @return
	 */
	@Deprecated
	public List<ScoredResult> getResults() {
		return queryResponse.getResultsList();
	}

	public List<CompleteResult> getCompleteResults() {
		return queryResponse.getResultsList().stream().map(CompleteResult::new).toList();
	}

	/**
	 * use getFirstCompleteResult instead
	 *
	 * @return
	 */
	@Deprecated()
	public ScoredResult getFirstResult() {
		if (hasResults()) {
			return getResults().get(0);
		}
		return null;
	}

	public CompleteResult getFirstCompleteResult() {
		if (hasResults()) {
			return new CompleteResult(queryResponse.getResultsList().get(0));
		}
		return null;
	}

	public List<Document> getDocuments() {
		List<Document> documents = new ArrayList<>();
		for (ScoredResult scoredResult : queryResponse.getResultsList()) {
			Document doc = ResultHelper.getDocumentFromScoredResult(scoredResult);
			if (doc != null) {
				documents.add(doc);
			}
			else {
				throw new IllegalStateException("Cannot get results without fetch type of full");
			}
		}
		return documents;
	}

	public List<String> getDocumentsAsJson() {
		List<String> jsons = new ArrayList<>();
		for (Document document : getDocuments()) {
			jsons.add(ZuliaUtil.mongoDocumentToJson(document));
		}
		return jsons;
	}

	public List<String> getDocumentsAsPrettyJson() {
		List<String> jsons = new ArrayList<>();
		for (Document document : getDocuments()) {
			jsons.add(ZuliaUtil.mongoDocumentToJson(document, true));
		}
		return jsons;
	}

	public List<Document> getMetaDocuments() {
		List<Document> documents = new ArrayList<>();
		for (ScoredResult scoredResult : queryResponse.getResultsList()) {
			Document metadata = ZuliaUtil.byteStringToMongoDocument(scoredResult.getResultDocument().getMetadata());
			if (metadata != null) {
				documents.add(metadata);
			}
			else {
				throw new IllegalStateException("Cannot get meta document without fetch type of meta or full");
			}
		}
		return documents;
	}

	public <T> List<T> getMappedDocuments(GsonDocumentMapper<T> mapper) throws Exception {
		List<T> items = new ArrayList<>();
		for (ScoredResult scoredResult : queryResponse.getResultsList()) {
			T item = mapper.fromScoredResult(scoredResult);
			if (item != null) {
				items.add(item);
			}
			else {
				throw new IllegalStateException("Cannot get results without fetch type of full");
			}
		}
		return items;
	}

	public Document getFirstDocument() {
		if (hasResults()) {
			Document doc = getFirstCompleteResult().getDocument();
			if (doc != null) {
				return doc;
			}
			else {
				throw new IllegalStateException("Cannot get results without fetch type of full");
			}
		}
		return null;
	}

	public String getFirstDocumentAsJson() {
		Document firstDocument = getFirstDocument();
		if (firstDocument != null) {
			return ZuliaUtil.mongoDocumentToJson(firstDocument);
		}
		return null;
	}

	public String getFirstDocumentAsPrettyJson() {
		Document firstDocument = getFirstDocument();
		if (firstDocument != null) {
			return ZuliaUtil.mongoDocumentToJson(firstDocument, true);
		}
		return null;
	}

	public LastResult getLastResult() {
		return queryResponse.getLastResult();
	}

	public List<FacetGroup> getFacetGroups() {
		return queryResponse.getFacetGroupList();
	}

	public List<FacetCount> getFacetCounts(String fieldName) {
		for (FacetGroup fg : queryResponse.getFacetGroupList()) {
			if (fieldName.equals(fg.getCountRequest().getFacetField().getLabel())) {
				return fg.getFacetCountList();
			}
		}
		return null;
	}

	public List<FacetCount> getFacetCountsForPath(String fieldName, String... paths) {
		return getFacetCountsForPath(fieldName, Arrays.asList(paths));
	}

	public List<FacetCount> getFacetCountsForPath(String fieldName, List<String> paths) {
		for (FacetGroup fg : queryResponse.getFacetGroupList()) {
			ZuliaQuery.Facet facetField = fg.getCountRequest().getFacetField();
			if (fieldName.equals(facetField.getLabel()) && facetField.getPathList().equals(paths)) {
				return fg.getFacetCountList();
			}
		}
		return null;
	}

	public int getFacetGroupCount() {
		return queryResponse.getFacetGroupCount();
	}

	public List<StatGroup> getStatGroups() {
		return queryResponse.getStatGroupList();
	}

	public List<StatGroup> getNumericFieldStats() {
		return queryResponse.getStatGroupList().stream().filter(sg -> sg.getStatRequest().getFacetField().getLabel().isEmpty()).collect(Collectors.toList());
	}

	public FacetStats getNumericFieldStat(String numericFieldName) {
		for (StatGroup sg : queryResponse.getStatGroupList()) {
			if (numericFieldName.equals(sg.getStatRequest().getNumericField()) && sg.getStatRequest().getFacetField().getLabel().isEmpty()) {
				return sg.getGlobalStats();
			}
		}
		return null;
	}

	public List<StatGroup> getFacetFieldStats() {
		return queryResponse.getStatGroupList().stream().filter(sg -> !sg.getStatRequest().getFacetField().getLabel().isEmpty()).collect(Collectors.toList());
	}

	public List<FacetStats> getFacetFieldStat(String numericFieldName, String facetField) {
		if (facetField == null) {
			facetField = "";
		}
		for (StatGroup sg : queryResponse.getStatGroupList()) {
			if (numericFieldName.equals(sg.getStatRequest().getNumericField()) && facetField.equals(sg.getStatRequest().getFacetField().getLabel())) {
				return sg.getFacetStatsList();
			}
		}
		return null;
	}

	public List<FacetStats> getFacetFieldStat(String numericFieldName, String facetField, List<String> paths) {
		if (facetField == null) {
			facetField = "";
		}
		for (StatGroup sg : queryResponse.getStatGroupList()) {
			if (numericFieldName.equals(sg.getStatRequest().getNumericField()) && facetField.equals(sg.getStatRequest().getFacetField().getLabel())
					&& paths.equals(sg.getStatRequest().getFacetField().getPathList())) {
				return sg.getFacetStatsList();
			}
		}
		return null;
	}

	public List<AnalysisResult> getSummaryAnalysisResults() {
		return queryResponse.getAnalysisResultList();
	}

	public boolean getFullyCached() {
		return queryResponse.getFullyCached();
	}

	public int getShardsCached() {
		return queryResponse.getShardsCached();
	}

	@Override
	public String toString() {
		return queryResponse.toString();
	}

}
