package io.zulia.server.config;

import io.zulia.message.ZuliaIndex.AnalyzerSettings;
import io.zulia.message.ZuliaIndex.IndexSettings;
import io.zulia.message.ZuliaServiceOuterClass.QueryRequest;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ServerIndexConfig {

	public static final int DEFAULT_RAM_BUFFER_MB = 128;
	public static final int DEFAULT_NRT_INDEX_MAX_MERGE_SIZE_MB = 50;
	public static final int DEFAULT_NRT_INDEX_MAX_CACHED_MB = 150;
	public static final int DEFAULT_NRT_TAXO_MAX_MERGE_SIZE_MB = 5;
	public static final int DEFAULT_NRT_TAXO_MAX_CACHED_MB = 15;

	// volatile so a live configure() (reloadIndexSettings) is promptly visible to concurrent reader/indexing
	// threads; the ServerIndexConfigData fields are all final, so safe publication is already covered.
	private volatile ServerIndexConfigData serverIndexConfigData;

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
		int value = getIndexSettings().getRamBufferMB();
		return value != 0 ? value : DEFAULT_RAM_BUFFER_MB;
	}

	public int getNrtIndexMaxMergeSizeMB() {
		int value = getIndexSettings().getNrtIndexMaxMergeSizeMB();
		return value != 0 ? value : DEFAULT_NRT_INDEX_MAX_MERGE_SIZE_MB;
	}

	public int getNrtIndexMaxCachedMB() {
		int value = getIndexSettings().getNrtIndexMaxCachedMB();
		return value != 0 ? value : DEFAULT_NRT_INDEX_MAX_CACHED_MB;
	}

	public int getNrtTaxoMaxMergeSizeMB() {
		int value = getIndexSettings().getNrtTaxoMaxMergeSizeMB();
		return value != 0 ? value : DEFAULT_NRT_TAXO_MAX_MERGE_SIZE_MB;
	}

	public int getNrtTaxoMaxCachedMB() {
		int value = getIndexSettings().getNrtTaxoMaxCachedMB();
		return value != 0 ? value : DEFAULT_NRT_TAXO_MAX_CACHED_MB;
	}

	public boolean isNrtCachingDisabled() {
		return getIndexSettings().getNrtCachingDisabled();
	}

	public int getMaxMergeThreads() {
		return getIndexSettings().getMaxMergeThreads();
	}

	public int getMaxMergePending() {
		return getIndexSettings().getMaxMergePending();
	}

	public int getIndexingThrottle() {
		return getIndexSettings().getIndexingThrottle();
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
