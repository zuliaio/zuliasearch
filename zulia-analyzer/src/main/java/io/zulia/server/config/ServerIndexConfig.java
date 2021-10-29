package io.zulia.server.config;

import info.debatty.java.lsh.SuperBit;
import io.zulia.DefaultAnalyzers;
import io.zulia.message.ZuliaIndex;
import io.zulia.message.ZuliaIndex.AnalyzerSettings;
import io.zulia.message.ZuliaIndex.AnalyzerSettings.Filter;
import io.zulia.message.ZuliaIndex.AnalyzerSettings.Tokenizer;
import io.zulia.message.ZuliaIndex.FacetAs;
import io.zulia.message.ZuliaIndex.FieldConfig;
import io.zulia.message.ZuliaIndex.IndexAs;
import io.zulia.message.ZuliaIndex.IndexSettings;
import io.zulia.message.ZuliaIndex.SortAs;

import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import static io.zulia.message.ZuliaIndex.ProjectAs;
import static io.zulia.message.ZuliaIndex.Superbit;

public class ServerIndexConfig {

	private IndexSettings indexSettings;

	private ConcurrentHashMap<String, FieldConfig> fieldConfigMap;
	private ConcurrentHashMap<String, IndexAs> indexAsMap;
	private ConcurrentHashMap<String, FieldConfig.FieldType> indexFieldType;
	private ConcurrentHashMap<String, FieldConfig.FieldType> sortFieldType;
	private ConcurrentHashMap<String, AnalyzerSettings> analyzerMap;
	private ConcurrentHashMap<String, String> indexToStoredMap;
	private ConcurrentHashMap<String, FacetAs> facetAsMap;
	private ConcurrentHashMap<String, ZuliaIndex.Superbit> superbitConfigMap;
	private ConcurrentHashMap<String, SuperBit> superbitMap;

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

		this.superbitConfigMap = new ConcurrentHashMap<>();
		this.superbitMap = new ConcurrentHashMap<>();
		for (String storedFieldName : fieldConfigMap.keySet()) {
			FieldConfig fc = fieldConfigMap.get(storedFieldName);
			for (ProjectAs projectAs : fc.getProjectAsList()) {
				String field = projectAs.getField();
				if (projectAs.hasSuperbit()) {
					Superbit superbit = projectAs.getSuperbit();
					superbitConfigMap.put(field, superbit);

					SuperBit superBit = new SuperBit(superbit.getInputDim(), superbit.getInputDim(), superbit.getBatches(), superbit.getSeed());
					superbitMap.put(field, superBit);
				}
			}
		}
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

	public SuperBit getSuperBitForField(String field) {
		return superbitMap.get(field);
	}

	public Superbit getSuperBitConfigForField(String field) {
		return superbitConfigMap.get(field);
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

	public Set<String> getMatchingFields(String regex) {
		Set<String> matchingFieldNames = new TreeSet<>();

		Pattern pattern = Pattern.compile(regex);
		for (String indexFieldName : indexAsMap.keySet()) {
			if (pattern.matcher(indexFieldName).matches()) {
				matchingFieldNames.add(indexFieldName);
			}
		}
		return matchingFieldNames;

	}

	@Override
	public String toString() {
		return "ServerIndexConfig{" + "indexSettings=" + indexSettings + '}';
	}

}
