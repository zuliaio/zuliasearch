package io.zulia.server.config;

import io.zulia.message.ZuliaIndex.AnalyzerSettings;
import io.zulia.message.ZuliaIndex.IndexSettings;
import io.zulia.message.ZuliaServiceOuterClass.QueryRequest;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ServerIndexConfig {

	private ServerIndexConfigData serverIndexConfigData;

	public ServerIndexConfig(IndexSettings indexSettings) {
		configure(indexSettings);
	}

	public void configure(IndexSettings indexSettings) {
		this.serverIndexConfigData = new ServerIndexConfigData(indexSettings);
	}

	public IndexSettings getIndexSettings() {
		return serverIndexConfigData.getIndexSettings();
	}

	public boolean existingFacet(String facet) {
		return serverIndexConfigData.existingFacet(facet);
	}

	public AnalyzerSettings getAnalyzerSettingsByName(String textAnalyzerName) {
		return serverIndexConfigData.getAnalyzerSettingsByName(textAnalyzerName);
	}

	public SortFieldInfo getSortFieldInfo(String sortField) {
		return serverIndexConfigData.getSortFieldInfo(sortField);
	}

	public IndexFieldInfo getIndexFieldInfo(String field) {
		return serverIndexConfigData.getIndexFieldInfo(field);
	}

	public Collection<String> getIndexedFields() {
		return serverIndexConfigData.getIndexedFields();
	}

	public int getNumberOfShards() {
		return getIndexSettings().getNumberOfShards();
	}

	public String getIndexName() {
		return getIndexSettings().getIndexName();
	}

	public int getRAMBufferMB() {
		return getIndexSettings().getRamBufferMB();
	}

	public boolean isCompressionEnabled() {
		return !getIndexSettings().getDisableCompression();
	}

	public Set<String> getMatchingFields(String field) {
		return serverIndexConfigData.getMatchingIndexFields(field, true);
	}

	public int getDefaultConcurrency() {
		return getIndexSettings().getDefaultConcurrency();
	}

	public List<QueryRequest> getWarmingSearches() {
		return serverIndexConfigData.getWarmingSearches();
	}

	public boolean isStoredIndividually(String facetField) {
		return serverIndexConfigData.isStoredIndividually(facetField);
	}

	public Map<String, Set<String>> getFacetGroups() {
		return serverIndexConfigData.getFacetGroups();
	}

	public String getFacetGroupForFacets(Set<String> facets) {
		return serverIndexConfigData.getFacetGroupForFacets(facets);
	}

	@Override
	public String toString() {
		return "ServerIndexConfig{" + "indexSettings=" + getIndexSettings() + '}';
	}

}
