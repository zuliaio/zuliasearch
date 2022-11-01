package io.zulia.server.config;

import com.google.protobuf.ByteString;
import io.zulia.DefaultAnalyzers;
import io.zulia.ZuliaConstants;
import io.zulia.message.ZuliaIndex.AnalyzerSettings;
import io.zulia.message.ZuliaIndex.AnalyzerSettings.Filter;
import io.zulia.message.ZuliaIndex.AnalyzerSettings.Tokenizer;
import io.zulia.message.ZuliaIndex.FacetAs;
import io.zulia.message.ZuliaIndex.FieldConfig;
import io.zulia.message.ZuliaIndex.IndexAs;
import io.zulia.message.ZuliaIndex.IndexSettings;
import io.zulia.message.ZuliaIndex.SortAs;
import io.zulia.message.ZuliaServiceOuterClass;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;
import java.util.regex.Pattern;

public class ServerIndexConfig {
	private final static Logger LOG = Logger.getLogger(ServerIndexConfig.class.getSimpleName());
	private IndexSettings indexSettings;

	private ConcurrentHashMap<String, FieldConfig> fieldConfigMap;
	private ConcurrentHashMap<String, IndexAs> indexAsMap;
	private ConcurrentHashMap<String, FieldConfig.FieldType> indexFieldType;
	private ConcurrentHashMap<String, FieldConfig.FieldType> sortFieldType;
	private ConcurrentHashMap<String, AnalyzerSettings> analyzerMap;
	private ConcurrentHashMap<String, String> indexToStoredMap;
	private ConcurrentHashMap<String, FacetAs> facetAsMap;

	private List<ZuliaServiceOuterClass.QueryRequest> warmingSearches;

	public ServerIndexConfig(IndexSettings indexSettings) {
		configure(indexSettings);
	}

	public void configure(IndexSettings indexSettings) {
		this.indexSettings = indexSettings;

		this.analyzerMap = new ConcurrentHashMap<>();

		analyzerMap.put(DefaultAnalyzers.STANDARD,
				AnalyzerSettings.newBuilder().setName(DefaultAnalyzers.STANDARD).addFilter(Filter.LOWERCASE).addFilter(Filter.STOPWORDS).build());
		analyzerMap.put(DefaultAnalyzers.KEYWORD, AnalyzerSettings.newBuilder().setName(DefaultAnalyzers.KEYWORD).setTokenizer(Tokenizer.KEYWORD).build());
		analyzerMap.put(DefaultAnalyzers.LC_KEYWORD,
				AnalyzerSettings.newBuilder().setName(DefaultAnalyzers.LC_KEYWORD).setTokenizer(Tokenizer.KEYWORD).addFilter(Filter.LOWERCASE).build());
		analyzerMap.put(DefaultAnalyzers.MIN_STEM,
				AnalyzerSettings.newBuilder().setName(DefaultAnalyzers.MIN_STEM).setTokenizer(Tokenizer.STANDARD).addFilter(Filter.LOWERCASE)
						.addFilter(Filter.STOPWORDS).addFilter(Filter.ENGLISH_MIN_STEM).build());

		analyzerMap.put(DefaultAnalyzers.TWO_TWO_SHINGLE,
				AnalyzerSettings.newBuilder().setName(DefaultAnalyzers.TWO_TWO_SHINGLE).setTokenizer(Tokenizer.STANDARD).addFilter(Filter.LOWERCASE)
						.addFilter(Filter.TWO_TWO_SHINGLE).build());
		analyzerMap.put(DefaultAnalyzers.THREE_THREE_SHINGLE,
				AnalyzerSettings.newBuilder().setName(DefaultAnalyzers.THREE_THREE_SHINGLE).setTokenizer(Tokenizer.STANDARD).addFilter(Filter.LOWERCASE)
						.addFilter(Filter.THREE_THREE_SHINGLE).build());

		analyzerMap.put(DefaultAnalyzers.LC_CONCAT_ALL,
				AnalyzerSettings.newBuilder().setName(DefaultAnalyzers.LC_CONCAT_ALL).setTokenizer(Tokenizer.KEYWORD).addFilter(Filter.LOWERCASE)
						.addFilter(Filter.CONCAT_ALL).build());

		analyzerMap.put(DefaultAnalyzers.KSTEMMED,
				AnalyzerSettings.newBuilder().setName(DefaultAnalyzers.KSTEMMED).setTokenizer(Tokenizer.STANDARD).addFilter(Filter.LOWERCASE)
						.addFilter(Filter.STOPWORDS).addFilter(Filter.KSTEM).build());
		analyzerMap.put(DefaultAnalyzers.LSH,
				AnalyzerSettings.newBuilder().setName(DefaultAnalyzers.LSH).setTokenizer(Tokenizer.STANDARD).addFilter(Filter.LOWERCASE)
						.addFilter(Filter.ASCII_FOLDING).addFilter(Filter.KSTEM).addFilter(Filter.STOPWORDS).addFilter(Filter.FIVE_FIVE_SHINGLE)
						.addFilter(Filter.MINHASH).build());

		for (AnalyzerSettings analyzerSettings : indexSettings.getAnalyzerSettingsList()) {
			analyzerMap.put(analyzerSettings.getName(), analyzerSettings);
		}

		this.fieldConfigMap = new ConcurrentHashMap<>();
		for (FieldConfig fc : indexSettings.getFieldConfigList()) {
			fieldConfigMap.put(fc.getStoredFieldName(), fc);
		}

		this.indexAsMap = new ConcurrentHashMap<>();
		this.indexToStoredMap = new ConcurrentHashMap<>();
		for (String storedFieldName : fieldConfigMap.keySet()) {
			FieldConfig fc = fieldConfigMap.get(storedFieldName);
			for (IndexAs indexAs : fc.getIndexAsList()) {
				indexAsMap.put(indexAs.getIndexFieldName(), indexAs);
				indexToStoredMap.put(indexAs.getIndexFieldName(), storedFieldName);
			}
		}

		this.facetAsMap = new ConcurrentHashMap<>();
		for (String storedFieldName : fieldConfigMap.keySet()) {
			FieldConfig fc = fieldConfigMap.get(storedFieldName);
			for (FacetAs facetAs : fc.getFacetAsList()) {
				facetAsMap.put(facetAs.getFacetName(), facetAs);
			}
		}

		this.indexFieldType = new ConcurrentHashMap<>();
		for (String storedFieldName : fieldConfigMap.keySet()) {
			FieldConfig fc = fieldConfigMap.get(storedFieldName);
			for (IndexAs indexAs : fc.getIndexAsList()) {
				indexFieldType.put(indexAs.getIndexFieldName(), fc.getFieldType());
			}
		}

		this.sortFieldType = new ConcurrentHashMap<>();
		for (String storedFieldName : fieldConfigMap.keySet()) {
			FieldConfig fc = fieldConfigMap.get(storedFieldName);
			for (SortAs sortAs : fc.getSortAsList()) {
				sortFieldType.put(sortAs.getSortFieldName(), fc.getFieldType());
			}
		}

		sortFieldType.put(ZuliaConstants.SCORE_FIELD, FieldConfig.FieldType.NUMERIC_FLOAT);
		sortFieldType.put(ZuliaConstants.ID_SORT_FIELD, FieldConfig.FieldType.STRING);

		this.warmingSearches = new ArrayList<>();
		for (ByteString bytes : indexSettings.getWarmingSearchesList()) {
			try {
				ZuliaServiceOuterClass.QueryRequest queryRequest = ZuliaServiceOuterClass.QueryRequest.parseFrom(bytes);
				warmingSearches.add(queryRequest);
			}
			catch (Exception e) {
				//Allow index to load vs throwing an exception and making this harder to fix with the index not loaded
				LOG.severe("Failed to load warming search: " + e.getMessage() + ".  Please store warming searches again in proper format.");
			}
		}

	}

	public String getSortField(String sortFieldName, FieldConfig.FieldType fieldType) {
		return sortFieldName + ZuliaConstants.SORT_SUFFIX + fieldType;
	}

	public IndexSettings getIndexSettings() {
		return indexSettings;
	}

	public AnalyzerSettings getAnalyzerSettingsForIndexField(String fieldName) {
		IndexAs indexAs = indexAsMap.get(fieldName);
		if (indexAs != null) {

			String textAnalyzerName = indexAs.getAnalyzerName();
			return getAnalyzerSettingsByName(textAnalyzerName);
		}
		return null;
	}

	public Set<String> getFacetFields() {
		return facetAsMap.keySet();
	}

	public boolean existingFacet(String facet) {
		return facetAsMap.containsKey(facet);
	}

	public boolean isHierarchicalFacet(String facet) {
		return facetAsMap.get(facet).getHierarchical();
	}

	public AnalyzerSettings getAnalyzerSettingsByName(String textAnalyzerName) {
		return analyzerMap.get(textAnalyzerName);
	}

	public FieldConfig.FieldType getFieldTypeForIndexField(String fieldName) {
		return indexFieldType.get(fieldName);
	}

	public FieldConfig.FieldType getFieldTypeForSortField(String sortField) {
		return sortFieldType.get(sortField);
	}

	public Collection<IndexAs> getIndexAsValues() {
		return indexAsMap.values();
	}

	public FieldConfig getFieldConfig(String storedFieldName) {
		return fieldConfigMap.get(storedFieldName);
	}

	public String getStoredFieldName(String indexFieldName) {
		return indexToStoredMap.get(indexFieldName);
	}

	public Set<String> getIndexedStoredFieldNames() {
		return fieldConfigMap.keySet();
	}

	public int getNumberOfShards() {
		return indexSettings.getNumberOfShards();
	}

	public String getIndexName() {
		return indexSettings.getIndexName();
	}

	public int getRAMBufferMB() {
		return indexSettings.getRamBufferMB();
	}

	public Set<String> getMatchingFields(String fieldWildcard) {

		if (fieldWildcard.contains("*")) {

			fieldWildcard = ("\\Q" + fieldWildcard + "\\E").replace("*", "\\E.*\\Q");

			Set<String> matchingFieldNames = new TreeSet<>();

			Pattern pattern = Pattern.compile(fieldWildcard);
			for (String indexFieldName : indexAsMap.keySet()) {
				if (pattern.matcher(indexFieldName).matches()) {
					matchingFieldNames.add(indexFieldName);
				}
			}
			return matchingFieldNames;
		}
		return Set.of(fieldWildcard);

	}

	public List<ZuliaServiceOuterClass.QueryRequest> getWarmingSearches() {
		return warmingSearches;
	}

	@Override
	public String toString() {
		return "ServerIndexConfig{" + "indexSettings=" + indexSettings + '}';
	}

}
