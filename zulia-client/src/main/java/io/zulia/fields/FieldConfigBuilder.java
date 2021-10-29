package io.zulia.fields;

import java.util.ArrayList;
import java.util.List;

import static io.zulia.message.ZuliaIndex.FacetAs;
import static io.zulia.message.ZuliaIndex.FieldConfig;
import static io.zulia.message.ZuliaIndex.IndexAs;
import static io.zulia.message.ZuliaIndex.ProjectAs;
import static io.zulia.message.ZuliaIndex.SortAs;
import static io.zulia.message.ZuliaIndex.Superbit;

public class FieldConfigBuilder {
	private final FieldConfig.FieldType fieldType;
	private final String storedFieldName;
	private final List<IndexAs> indexAsList;
	private final List<FacetAs> facetAsList;
	private final List<SortAs> sortAsList;
	private final List<ProjectAs> projectAsList;
	private String description;
	private String displayName;

	public FieldConfigBuilder(String storedFieldName, FieldConfig.FieldType fieldType) {
		this.storedFieldName = storedFieldName;
		this.fieldType = fieldType;
		this.indexAsList = new ArrayList<>();
		this.facetAsList = new ArrayList<>();
		this.sortAsList = new ArrayList<>();
		this.projectAsList = new ArrayList<>();
	}

	public static FieldConfigBuilder create(String storedFieldName, FieldConfig.FieldType fieldType) {
		return new FieldConfigBuilder(storedFieldName, fieldType);
	}

	public FieldConfigBuilder index() {
		return indexAs(null, storedFieldName);
	}

	public FieldConfigBuilder indexAs(String analyzerName) {
		return indexWithAnalyzer(analyzerName);
	}

	public FieldConfigBuilder indexWithAnalyzer(String analyzerName) {
		return indexAs(analyzerName, storedFieldName);
	}

	public FieldConfigBuilder indexAsField(String fieldName) {
		return indexAs(null, fieldName);
	}

	public FieldConfigBuilder indexAs(String analyzerName, String indexedFieldName) {

		IndexAs.Builder builder = IndexAs.newBuilder();
		builder.setIndexFieldName(indexedFieldName);
		if (analyzerName != null) {
			builder.setAnalyzerName(analyzerName);
		}
		return indexAs(builder.build());
	}

	public FieldConfigBuilder indexAs(IndexAs indexAs) {
		this.indexAsList.add(indexAs);
		return this;
	}

	public FieldConfigBuilder facet() {
		FacetAs.Builder builder = FacetAs.newBuilder().setFacetName(storedFieldName).setHierarchical(false);
		return facetAs(builder.build());
	}

	public FieldConfigBuilder facetHierarchical() {
		FacetAs.Builder builder = FacetAs.newBuilder().setFacetName(storedFieldName).setHierarchical(true);
		return facetAs(builder.build());
	}

	public FieldConfigBuilder facetAs(String facetName) {
		FacetAs.Builder builder = FacetAs.newBuilder().setFacetName(facetName).setHierarchical(false);
		return facetAs(builder.build());
	}

	public FieldConfigBuilder facetAsHierarchical(String facetName) {
		FacetAs.Builder builder = FacetAs.newBuilder().setFacetName(facetName).setHierarchical(true);
		return facetAs(builder.build());
	}

	public FieldConfigBuilder facetAs(FacetAs.DateHandling dateHandling) {
		FacetAs.Builder builder = FacetAs.newBuilder().setFacetName(storedFieldName).setDateHandling(dateHandling).setHierarchical(false);
		return facetAs(builder.build());
	}

	public FieldConfigBuilder facetAsHierarchical(FacetAs.DateHandling dateHandling) {
		FacetAs.Builder builder = FacetAs.newBuilder().setFacetName(storedFieldName).setDateHandling(dateHandling).setHierarchical(true);
		return facetAs(builder.build());
	}

	public FieldConfigBuilder facetAs(String facetName, FacetAs.DateHandling dateHandling) {
		FacetAs.Builder builder = FacetAs.newBuilder().setFacetName(facetName).setDateHandling(dateHandling).setHierarchical(false);
		return facetAs(builder.build());
	}

	public FieldConfigBuilder facetAsHierarchical(String facetName, FacetAs.DateHandling dateHandling) {
		FacetAs.Builder builder = FacetAs.newBuilder().setFacetName(facetName).setDateHandling(dateHandling).setHierarchical(true);
		return facetAs(builder.build());
	}

	public FieldConfigBuilder facetAs(FacetAs facetAs) {
		this.facetAsList.add(facetAs);
		return this;
	}

	public FieldConfigBuilder sort() {
		return sortAs(null, storedFieldName);
	}

	public FieldConfigBuilder sortAs(SortAs.StringHandling stringHandling) {
		return sortAs(stringHandling, storedFieldName);
	}

	public FieldConfigBuilder sortAs(SortAs.StringHandling stringHandling, String sortFieldName) {
		SortAs.Builder builder = SortAs.newBuilder().setSortFieldName(sortFieldName);
		if (stringHandling != null) {
			builder.setStringHandling(stringHandling);
		}
		return sortAs(builder.build());
	}

	public FieldConfigBuilder sortAs(SortAs sortAs) {
		this.sortAsList.add(sortAs);
		return this;
	}

	public FieldConfigBuilder projectAsSuperBit(String field, int inputDim) {
		Superbit superbit = Superbit.newBuilder().setInputDim(inputDim).build();
		ProjectAs projectAs = ProjectAs.newBuilder().setField(field).setSuperbit(superbit).build();
		return projectAs(projectAs);
	}

	public FieldConfigBuilder projectAsSuperBit(String field, int inputDim, int batches) {
		Superbit superbit = Superbit.newBuilder().setInputDim(inputDim).setBatches(batches).build();
		ProjectAs projectAs = ProjectAs.newBuilder().setField(field).setSuperbit(superbit).build();
		return projectAs(projectAs);
	}

	public FieldConfigBuilder projectAsSuperBit(String field, int inputDim, int batches, int seed) {
		Superbit superbit = Superbit.newBuilder().setInputDim(inputDim).setBatches(batches).setSeed(seed).build();
		ProjectAs projectAs = ProjectAs.newBuilder().setField(field).setSuperbit(superbit).build();
		return projectAs(projectAs);
	}

	public FieldConfigBuilder projectAsSuperBit(String field, Superbit superbit) {
		ProjectAs projectAs = ProjectAs.newBuilder().setField(field).setSuperbit(superbit).build();
		return projectAs(projectAs);
	}

	public FieldConfigBuilder projectAs(ProjectAs projectAs) {
		this.projectAsList.add(projectAs);
		return this;
	}

	public FieldConfigBuilder description(String description) {
		this.description = description;
		return this;
	}

	public FieldConfigBuilder displayName(String displayName) {
		this.displayName = displayName;
		return this;
	}

	public FieldConfig build() {
		FieldConfig.Builder fcBuilder = FieldConfig.newBuilder();
		fcBuilder.setStoredFieldName(storedFieldName);
		fcBuilder.setFieldType(fieldType);
		fcBuilder.addAllIndexAs(indexAsList);
		fcBuilder.addAllFacetAs(facetAsList);
		fcBuilder.addAllSortAs(sortAsList);
		fcBuilder.addAllProjectAs(projectAsList);
		if (description != null) {
			fcBuilder.setDescription(description);
		}
		if (displayName != null) {
			fcBuilder.setDisplayName(displayName);
		}
		return fcBuilder.build();
	}

}
