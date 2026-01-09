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
import io.zulia.message.ZuliaIndex.SortAs.StringHandling;
import io.zulia.message.ZuliaServiceOuterClass.QueryRequest;
import io.zulia.server.field.FieldTypeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

public class ServerIndexConfigData {

	private static final Logger LOG = LoggerFactory.getLogger(ServerIndexConfigData.class);

	private final IndexSettings indexSettings;
	private final Map<String, IndexFieldInfo> indexFieldMapping;
	private final Map<String, SortFieldInfo> sortFieldMapping;
	private final Map<String, AnalyzerSettings> analyzerMap;
	private final Map<String, Boolean> facetAsMap;
	private final List<QueryRequest> warmingSearches;
	private final Map<String, Set<String>> fieldMappingToFields;
	private final LinkedHashMap<String, Set<String>> facetGroupToFacets;
	private final Set<String> individualFacets;

	public ServerIndexConfigData(IndexSettings indexSettings) {
		this.indexSettings = indexSettings;
		this.analyzerMap = populateAnalyzers();
		this.indexFieldMapping = new HashMap<>();
		this.sortFieldMapping = new HashMap<>();
		this.facetAsMap = new HashMap<>();
		HashMap<String, Set<String>> facetGroupToFacetsTemp = new HashMap<>();
		this.individualFacets = new HashSet<>();

		for (FieldConfig fc : indexSettings.getFieldConfigList()) {
			String storedFieldName = fc.getStoredFieldName();
			FieldType fieldType = fc.getFieldType();

			List<SortFieldInfo> sortFieldInfos = new ArrayList<>(fc.getSortAsCount());
			for (SortAs sortAs : fc.getSortAsList()) {
				SortFieldInfo sortFieldInfo = new SortFieldInfo(FieldTypeUtil.getSortField(sortAs.getSortFieldName(), fieldType), fieldType,
						sortAs.getStringHandling());
				sortFieldInfos.add(sortFieldInfo);
				sortFieldMapping.put(sortAs.getSortFieldName(), sortFieldInfo);
			}

			for (IndexAs indexAs : fc.getIndexAsList()) {
				String indexFieldName = indexAs.getIndexFieldName();
				String listLengthWrap = FieldTypeUtil.getListLengthWrap(indexFieldName);
				String listLengthIndexField = FieldTypeUtil.getListLengthIndexField(indexFieldName);
				String listLengthSortField = FieldTypeUtil.getListLengthSortField(indexFieldName);

				indexFieldMapping.put(listLengthWrap,
						new IndexFieldInfo(storedFieldName, listLengthIndexField, listLengthSortField, FieldType.NUMERIC_INT, indexAs));
				sortFieldMapping.put(listLengthWrap, new SortFieldInfo(listLengthSortField, FieldType.NUMERIC_INT, null));

				String internalSortFieldName = null;

				if (FieldTypeUtil.isStringFieldType(fieldType)) {
					String charLengthWrap = FieldTypeUtil.getCharLengthWrap(indexFieldName);
					String charLengthIndexField = FieldTypeUtil.getCharLengthIndexField(indexFieldName);
					String charLengthSortField = FieldTypeUtil.getCharLengthSortField(indexFieldName);
					indexFieldMapping.put(charLengthWrap,
							new IndexFieldInfo(storedFieldName, charLengthIndexField, charLengthSortField, FieldType.NUMERIC_INT, indexAs));
					sortFieldMapping.put(charLengthWrap, new SortFieldInfo(charLengthSortField, FieldType.NUMERIC_INT, null));

					//only optimize a keyword analyzer with a standard (no-op) string handling sort field
					if (DefaultAnalyzers.KEYWORD.equals(indexAs.getAnalyzerName())) {
						for (SortFieldInfo sortFieldInfo : sortFieldInfos) {
							if (StringHandling.STANDARD.equals(sortFieldInfo.getStringHandling())) {
								internalSortFieldName = sortFieldInfo.getInternalSortFieldName();
								break;
							}
						}
					}
					//only optimize a lowercase keyword analyzer with a lowercase string handling sort field
					else if (DefaultAnalyzers.LC_KEYWORD.equals(indexAs.getAnalyzerName())) {
						for (SortFieldInfo sortFieldInfo : sortFieldInfos) {
							if (StringHandling.LOWERCASE.equals(sortFieldInfo.getStringHandling())) {
								internalSortFieldName = sortFieldInfo.getInternalSortFieldName();
								break;
							}
						}
					}

				}
				else {
					//for numerics, we can just use the first sort field because they are all the same
					if (!sortFieldInfos.isEmpty()) {
						internalSortFieldName = sortFieldInfos.get(0).getInternalSortFieldName();
					}
				}

				String indexField = FieldTypeUtil.getIndexField(indexFieldName, fieldType);
				IndexFieldInfo indexFieldInfo = new IndexFieldInfo(storedFieldName, indexField, internalSortFieldName, fieldType, indexAs);
				indexFieldMapping.put(indexFieldName, indexFieldInfo);
			}

			for (FacetAs facetAs : fc.getFacetAsList()) {
				String facetName = facetAs.getFacetName();
				facetAsMap.put(facetName, true);
				if (facetAs.getStoreInOwnGroup()) {
					individualFacets.add(facetName);
				}
				for (String facetGroup : facetAs.getFacetGroupsList()) {
					facetGroupToFacetsTemp.computeIfAbsent(facetGroup, s -> new HashSet<>()).add(facetName);
				}
			}
		}

		indexFieldMapping.put(ZuliaFieldConstants.ID_FIELD, new IndexFieldInfo(null, ZuliaFieldConstants.ID_FIELD,
				FieldTypeUtil.getSortField(ZuliaFieldConstants.ID_SORT_FIELD, FieldConfig.FieldType.STRING), FieldType.STRING, null));

		indexFieldMapping.put(ZuliaFieldConstants.TIMESTAMP_FIELD, new IndexFieldInfo(null, ZuliaFieldConstants.TIMESTAMP_FIELD, null, FieldType.DATE, null));

		indexFieldMapping.put(ZuliaFieldConstants.FACET_DRILL_DOWN_FIELD,
				new IndexFieldInfo(null, ZuliaFieldConstants.FACET_DRILL_DOWN_FIELD, null, FieldType.STRING, null));
		indexFieldMapping.put(ZuliaFieldConstants.FIELDS_LIST_FIELD,
				new IndexFieldInfo(null, ZuliaFieldConstants.FIELDS_LIST_FIELD, null, FieldType.STRING, null));
		sortFieldMapping.put(ZuliaFieldConstants.SCORE_FIELD, new SortFieldInfo(null, FieldType.NUMERIC_FLOAT, null));
		sortFieldMapping.put(ZuliaFieldConstants.ID_SORT_FIELD,
				new SortFieldInfo(FieldTypeUtil.getSortField(ZuliaFieldConstants.ID_SORT_FIELD, FieldConfig.FieldType.STRING), FieldType.STRING,
						StringHandling.STANDARD));

		this.warmingSearches = new ArrayList<>();
		for (ByteString bytes : indexSettings.getWarmingSearchesList()) {
			try {
				QueryRequest queryRequest = QueryRequest.parseFrom(bytes);
				warmingSearches.add(queryRequest);
			}
			catch (Exception e) {
				//Allow index to load vs throwing an exception and making this harder to fix with the index not loaded
				LOG.error("Failed to load warming search: {}.  Please store warming searches again in proper format.", e.getMessage(), e);
			}
		}

		this.fieldMappingToFields = new HashMap<>();

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

		// reorder so that the smallest facet groups are first to optimize the smallest covering facet group when selecting
		ArrayList<Map.Entry<String, Set<String>>> facetGroupEntries = new ArrayList<>(facetGroupToFacetsTemp.entrySet());
		facetGroupEntries.sort(Map.Entry.comparingByValue(Comparator.comparingInt(Set::size)));
		this.facetGroupToFacets = new LinkedHashMap<>();
		for (Map.Entry<String, Set<String>> facetGroupEntry : facetGroupEntries) {
			facetGroupToFacets.put(facetGroupEntry.getKey(), facetGroupEntry.getValue());
		}

	}

	private Map<String, AnalyzerSettings> populateAnalyzers() {
		Map<String, AnalyzerSettings> aMap = new HashMap<>();

		aMap.put(DefaultAnalyzers.STANDARD,
				AnalyzerSettings.newBuilder().setName(DefaultAnalyzers.STANDARD).addFilter(Filter.LOWERCASE).addFilter(Filter.STOPWORDS).build());
		aMap.put(DefaultAnalyzers.STANDARD_HTML,
				AnalyzerSettings.newBuilder().setName(DefaultAnalyzers.STANDARD_HTML).addFilter(Filter.LOWERCASE).addFilter(Filter.STOPWORDS).setStripHTML(true)
						.build());
		aMap.put(DefaultAnalyzers.KEYWORD, AnalyzerSettings.newBuilder().setName(DefaultAnalyzers.KEYWORD).setTokenizer(Tokenizer.KEYWORD).build());
		aMap.put(DefaultAnalyzers.LC_KEYWORD,
				AnalyzerSettings.newBuilder().setName(DefaultAnalyzers.LC_KEYWORD).setTokenizer(Tokenizer.KEYWORD).addFilter(Filter.LOWERCASE).build());
		aMap.put(DefaultAnalyzers.MIN_STEM,
				AnalyzerSettings.newBuilder().setName(DefaultAnalyzers.MIN_STEM).setTokenizer(Tokenizer.STANDARD).addFilter(Filter.LOWERCASE)
						.addFilter(Filter.STOPWORDS).addFilter(Filter.ENGLISH_MIN_STEM).build());

		aMap.put(DefaultAnalyzers.TWO_TWO_SHINGLE,
				AnalyzerSettings.newBuilder().setName(DefaultAnalyzers.TWO_TWO_SHINGLE).setTokenizer(Tokenizer.STANDARD).addFilter(Filter.LOWERCASE)
						.addFilter(Filter.TWO_TWO_SHINGLE).build());
		aMap.put(DefaultAnalyzers.THREE_THREE_SHINGLE,
				AnalyzerSettings.newBuilder().setName(DefaultAnalyzers.THREE_THREE_SHINGLE).setTokenizer(Tokenizer.STANDARD).addFilter(Filter.LOWERCASE)
						.addFilter(Filter.THREE_THREE_SHINGLE).build());

		aMap.put(DefaultAnalyzers.LC_CONCAT_ALL,
				AnalyzerSettings.newBuilder().setName(DefaultAnalyzers.LC_CONCAT_ALL).setTokenizer(Tokenizer.KEYWORD).addFilter(Filter.LOWERCASE)
						.addFilter(Filter.CONCAT_ALL).build());

		aMap.put(DefaultAnalyzers.KSTEMMED,
				AnalyzerSettings.newBuilder().setName(DefaultAnalyzers.KSTEMMED).setTokenizer(Tokenizer.STANDARD).addFilter(Filter.LOWERCASE)
						.addFilter(Filter.STOPWORDS).addFilter(Filter.KSTEM).build());
		aMap.put(DefaultAnalyzers.LSH, AnalyzerSettings.newBuilder().setName(DefaultAnalyzers.LSH).setTokenizer(Tokenizer.STANDARD).addFilter(Filter.LOWERCASE)
				.addFilter(Filter.ASCII_FOLDING).addFilter(Filter.KSTEM).addFilter(Filter.STOPWORDS).addFilter(Filter.FIVE_FIVE_SHINGLE)
				.addFilter(Filter.MINHASH).build());

		for (AnalyzerSettings analyzerSettings : indexSettings.getAnalyzerSettingsList()) {
			aMap.put(analyzerSettings.getName(), analyzerSettings);
		}
		return aMap;
	}

	protected Set<String> getMatchingIndexFields(String field, boolean includeAliases) {
		if (field.contains("*")) {

			field = ("\\Q" + field + "\\E").replace("*", "\\E.*\\Q");

			Set<String> matchingFieldNames = new TreeSet<>();

			Pattern pattern = Pattern.compile(field);
			for (String indexFieldName : indexFieldMapping.keySet()) {
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

	public IndexSettings getIndexSettings() {
		return indexSettings;
	}

	public boolean existingFacet(String facet) {
		return facetAsMap.containsKey(facet);
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

	public List<QueryRequest> getWarmingSearches() {
		return warmingSearches;
	}

	public boolean isStoredIndividually(String facetField) {
		return individualFacets.contains(facetField);
	}

	public Map<String, Set<String>> getFacetGroups() {
		return facetGroupToFacets;
	}

	public String getFacetGroupForFacets(Set<String> facets) {
		for (Map.Entry<String, Set<String>> facetGroupWithFacets : facetGroupToFacets.entrySet()) {
			if (facetGroupWithFacets.getValue().containsAll(facets)) {
				return facetGroupWithFacets.getKey();
			}
		}
		return null;
	}
}