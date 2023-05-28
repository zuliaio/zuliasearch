package io.zulia.server.config;

import com.google.protobuf.ByteString;
import io.zulia.DefaultAnalyzers;
import io.zulia.ZuliaFieldConstants;
import io.zulia.message.ZuliaIndex;
import io.zulia.message.ZuliaIndex.AnalyzerSettings;
import io.zulia.message.ZuliaIndex.AnalyzerSettings.Filter;
import io.zulia.message.ZuliaIndex.AnalyzerSettings.Tokenizer;
import io.zulia.message.ZuliaIndex.FacetAs;
import io.zulia.message.ZuliaIndex.FieldConfig;
import io.zulia.message.ZuliaIndex.FieldConfig.FieldType;
import io.zulia.message.ZuliaIndex.IndexAs;
import io.zulia.message.ZuliaIndex.IndexSettings;
import io.zulia.message.ZuliaIndex.SortAs;
import io.zulia.message.ZuliaServiceOuterClass;
import io.zulia.server.field.FieldTypeUtil;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;
import java.util.regex.Pattern;

public class ServerIndexConfig {
	private final static Logger LOG = Logger.getLogger(ServerIndexConfig.class.getSimpleName());
	private IndexSettings indexSettings;

	private ConcurrentHashMap<String, IndexFieldInfo> indexFieldMapping;
	private ConcurrentHashMap<String, SortFieldInfo> sortFieldMapping;
	private ConcurrentHashMap<String, AnalyzerSettings> analyzerMap;

	private ConcurrentHashMap<String, FacetAs> facetAsMap;

	private List<ZuliaServiceOuterClass.QueryRequest> warmingSearches;

	private ConcurrentHashMap<String, Set<String>> fieldMappingToFields;

	public ServerIndexConfig(IndexSettings indexSettings) {
		configure(indexSettings);
	}

	public void configure(IndexSettings indexSettings) {
		this.indexSettings = indexSettings;

		populateAnalyzers();

		this.indexFieldMapping = new ConcurrentHashMap<>();
		this.sortFieldMapping = new ConcurrentHashMap<>();
		this.facetAsMap = new ConcurrentHashMap<>();

		for (FieldConfig fc : indexSettings.getFieldConfigList()) {
			String storedFieldName = fc.getStoredFieldName();
			FieldType fieldType = fc.getFieldType();

			SortFieldInfo firstSortFieldInfo = null;
			for (SortAs sortAs : fc.getSortAsList()) {
				SortFieldInfo sortFieldInfo = new SortFieldInfo(FieldTypeUtil.getSortField(sortAs.getSortFieldName(), fieldType), fieldType);
				if (firstSortFieldInfo == null) {
					firstSortFieldInfo = sortFieldInfo;
				}
				sortFieldMapping.put(sortAs.getSortFieldName(), sortFieldInfo);
			}

			for (IndexAs indexAs : fc.getIndexAsList()) {
				String indexFieldName = indexAs.getIndexFieldName();
				String listLengthWrap = FieldTypeUtil.getListLengthWrap(indexFieldName);
				String listLengthIndexField = FieldTypeUtil.getListLengthIndexField(indexFieldName);
				String listLengthSortField = FieldTypeUtil.getListLengthSortField(indexFieldName);

				indexFieldMapping.put(listLengthWrap,
						new IndexFieldInfo(storedFieldName, listLengthIndexField, listLengthSortField, FieldType.NUMERIC_INT, indexAs));
				sortFieldMapping.put(listLengthWrap, new SortFieldInfo(listLengthSortField, FieldType.NUMERIC_INT));
				if (FieldTypeUtil.isStringFieldType(fieldType)) {
					String charLengthWrap = FieldTypeUtil.getCharLengthWrap(indexFieldName);
					String charLengthIndexField = FieldTypeUtil.getCharLengthIndexField(indexFieldName);
					String charLengthSortField = FieldTypeUtil.getCharLengthSortField(indexFieldName);
					indexFieldMapping.put(charLengthWrap,
							new IndexFieldInfo(storedFieldName, charLengthIndexField, charLengthSortField, FieldType.NUMERIC_INT, indexAs));
					sortFieldMapping.put(charLengthWrap, new SortFieldInfo(charLengthSortField, FieldType.NUMERIC_INT));
				}

				String internalSortFieldName = firstSortFieldInfo != null ? firstSortFieldInfo.getInternalSortFieldName() : null;
				String indexField = FieldTypeUtil.getIndexField(indexFieldName, fieldType);
				IndexFieldInfo indexFieldInfo = new IndexFieldInfo(storedFieldName, indexField, internalSortFieldName, fieldType, indexAs);
				indexFieldMapping.put(indexFieldName, indexFieldInfo);
			}

			for (FacetAs facetAs : fc.getFacetAsList()) {
				facetAsMap.put(facetAs.getFacetName(), facetAs);
			}
		}

		indexFieldMapping.put(ZuliaFieldConstants.ID_FIELD,
				new IndexFieldInfo(null, ZuliaFieldConstants.ID_FIELD, ZuliaFieldConstants.ID_SORT_FIELD, FieldType.STRING, null));
		indexFieldMapping.put(ZuliaFieldConstants.FACET_DRILL_DOWN_FIELD,
				new IndexFieldInfo(null, ZuliaFieldConstants.FACET_DRILL_DOWN_FIELD, null, FieldType.STRING, null));
		indexFieldMapping.put(ZuliaFieldConstants.FIELDS_LIST_FIELD,
				new IndexFieldInfo(null, ZuliaFieldConstants.FIELDS_LIST_FIELD, null, FieldType.STRING, null));
		sortFieldMapping.put(ZuliaFieldConstants.SCORE_FIELD, new SortFieldInfo(null, FieldType.NUMERIC_FLOAT));
		sortFieldMapping.put(ZuliaFieldConstants.ID_SORT_FIELD,
				new SortFieldInfo(FieldTypeUtil.getSortField(ZuliaFieldConstants.ID_SORT_FIELD, FieldConfig.FieldType.STRING), FieldType.STRING));

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

		this.fieldMappingToFields = new ConcurrentHashMap<>();

		for (ZuliaIndex.FieldMapping fieldMapping : indexSettings.getFieldMappingList()) {

			Set<String> matchingFields = new HashSet<>();
			for (String fieldOrFieldPattern : fieldMapping.getFieldOrFieldPatternList()) {
				matchingFields.addAll(getMatchingIndexFields(fieldOrFieldPattern, false));
			}

			if (fieldMapping.getIncludeSelf()) {
				matchingFields.add(fieldMapping.getAlias());
			}

			fieldMappingToFields.put(fieldMapping.getAlias(), matchingFields);
		}

	}

	private void populateAnalyzers() {
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
	}

	public IndexSettings getIndexSettings() {
		return indexSettings;
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

	public SortFieldInfo getSortFieldInfo(String sortField) {
		return sortFieldMapping.get(sortField);
	}

	public IndexFieldInfo getIndexFieldInfo(String field) {
		return indexFieldMapping.get(field);
	}

	public Collection<String> getIndexedFields() {
		return indexFieldMapping.keySet();
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

	public boolean isCompressionEnabled() {
		return !indexSettings.getDisableCompression();
	}

	public Set<String> getMatchingFields(String field) {
		return getMatchingIndexFields(field, true);
	}

	private Set<String> getMatchingIndexFields(String field, boolean includeAliases) {
		if (field.contains("*")) {

			field = ("\\Q" + field + "\\E").replace("*", "\\E.*\\Q");

			Set<String> matchingFieldNames = new TreeSet<>();

			Pattern pattern = Pattern.compile(field);
			for (String indexFieldName : getIndexedFields()) {
				if (pattern.matcher(indexFieldName).matches()) {
					matchingFieldNames.add(indexFieldName);
				}
			}

			if (includeAliases) {
				for (String alias : fieldMappingToFields.keySet()) {
					if (pattern.matcher(alias).matches()) {
						matchingFieldNames.addAll(fieldMappingToFields.get(alias));
					}
				}
			}

			return matchingFieldNames;
		}

		if (includeAliases && fieldMappingToFields.containsKey(field)) {
			return fieldMappingToFields.get(field);
		}

		return Set.of(field);
	}

	public List<ZuliaServiceOuterClass.QueryRequest> getWarmingSearches() {
		return warmingSearches;
	}

	@Override
	public String toString() {
		return "ServerIndexConfig{" + "indexSettings=" + indexSettings + '}';
	}

}
