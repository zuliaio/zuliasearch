package io.zulia.fields;

import java.util.ArrayList;
import java.util.List;

import static io.zulia.message.ZuliaIndex.FacetAs;
import static io.zulia.message.ZuliaIndex.FieldConfig;
import static io.zulia.message.ZuliaIndex.GeoPointConfig;
import static io.zulia.message.ZuliaIndex.IndexAs;
import static io.zulia.message.ZuliaIndex.SortAs;
import static io.zulia.message.ZuliaIndex.VectorDescription;
import static io.zulia.message.ZuliaIndex.VectorIndexingConfig;

public class FieldConfigBuilder {
	private final FieldConfig.FieldType fieldType;
	private final String storedFieldName;
	private final List<IndexAs> indexAsList;
	private final List<FacetAs> facetAsList;
	private final List<SortAs> sortAsList;
	private GeoPointConfig geoPointConfig;
	private VectorDescription.Builder vectorDescription;
	private VectorIndexingConfigBuilder vectorIndexingConfig;
	private String description;
	private String displayName;
	private Boolean docValueSkipIndex;

	public FieldConfigBuilder(String storedFieldName, FieldConfig.FieldType fieldType) {
		this.storedFieldName = storedFieldName;
		this.fieldType = fieldType;
		this.indexAsList = new ArrayList<>();
		this.facetAsList = new ArrayList<>();
		this.sortAsList = new ArrayList<>();
	}

	public static FieldConfigBuilder create(String storedFieldName, FieldConfig.FieldType fieldType) {
		return new FieldConfigBuilder(storedFieldName, fieldType);
	}

	public static FieldConfigBuilder createString(String storedFieldName) {
		return create(storedFieldName, FieldConfig.FieldType.STRING);
	}

	public static FieldConfigBuilder createBool(String storedFieldName) {
		return create(storedFieldName, FieldConfig.FieldType.BOOL);
	}

	public static FieldConfigBuilder createDate(String storedFieldName) {
		return create(storedFieldName, FieldConfig.FieldType.DATE);
	}

	public static FieldConfigBuilder createInt(String storedFieldName) {
		return create(storedFieldName, FieldConfig.FieldType.NUMERIC_INT);
	}

	public static FieldConfigBuilder createLong(String storedFieldName) {
		return create(storedFieldName, FieldConfig.FieldType.NUMERIC_LONG);
	}

	public static FieldConfigBuilder createFloat(String storedFieldName) {
		return create(storedFieldName, FieldConfig.FieldType.NUMERIC_FLOAT);
	}

	public static FieldConfigBuilder createDouble(String storedFieldName) {
		return create(storedFieldName, FieldConfig.FieldType.NUMERIC_DOUBLE);
	}

	/**
	 * Creates a VECTOR (cosine) field with no explicit encoding. The server picks one by index-creation version:
	 * INT8 scalar quantization for new indexes, raw float32 for legacy. Pin one with {@link #quantization(VectorIndexingConfig.Encoding)}.
	 */
	public static FieldConfigBuilder createVector(String storedFieldName) {
		return create(storedFieldName, FieldConfig.FieldType.VECTOR);
	}

	/** Creates a UNIT_VECTOR (dot-product) field. The encoding resolves as in {@link #createVector}. */
	public static FieldConfigBuilder createUnitVector(String storedFieldName) {
		return create(storedFieldName, FieldConfig.FieldType.UNIT_VECTOR);
	}

	public static FieldConfigBuilder createGeoPoint(String storedFieldName) {
		return createGeoPoint(storedFieldName, "latitude", "longitude");
	}

	public static FieldConfigBuilder createGeoPoint(String storedFieldName, String latitudeKey, String longitudeKey) {
		FieldConfigBuilder builder = create(storedFieldName, FieldConfig.FieldType.GEO_POINT);
		builder.geoPointConfig = GeoPointConfig.newBuilder().setLatitudeKey(latitudeKey).setLongitudeKey(longitudeKey).build();
		return builder;
	}

	/**
	 * Creates a GEO_POINT field where latitude and longitude are top-level document fields
	 * rather than nested inside a sub-document.
	 * <p>Example: {@code createGeoPointTopLevel("lat", "lon").indexAsField("position")}
	 * for documents like {@code {"lat": 40.7, "lon": -74.0}}
	 */
	public static FieldConfigBuilder createGeoPointTopLevel(String latitudeKey, String longitudeKey) {
		return createGeoPoint("", latitudeKey, longitudeKey);
	}

	private VectorDescription.Builder vectorDescriptionBuilder() {
		if (vectorDescription == null) {
			vectorDescription = VectorDescription.newBuilder();
		}
		return vectorDescription;
	}

	private VectorIndexingConfigBuilder vectorIndexingConfigBuilder() {
		if (vectorIndexingConfig == null) {
			vectorIndexingConfig = VectorIndexingConfigBuilder.create();
		}
		return vectorIndexingConfig;
	}

	/** Vector encoding for every indexed representation of this field that does not set its own via {@link #indexAs(String, VectorIndexingConfig.Encoding)}. */
	public FieldConfigBuilder quantization(VectorIndexingConfig.Encoding encoding) {
		vectorIndexingConfigBuilder().quantization(encoding);
		return this;
	}

	/** HNSW graph tuning (m = max connections, efConstruction = build beam width) for representations without their own config. */
	public FieldConfigBuilder hnsw(int m, int efConstruction) {
		vectorIndexingConfigBuilder().hnsw(m, efConstruction);
		return this;
	}

	/** Overrides the similarity (otherwise derived from field type: VECTOR=cosine, UNIT_VECTOR=dot-product). */
	public FieldConfigBuilder similarity(VectorDescription.Similarity similarity) {
		vectorDescriptionBuilder().setSimilarity(similarity);
		return this;
	}

	/** Expected vector dimensions, validated on store when set. */
	public FieldConfigBuilder dimensions(int dimensions) {
		vectorDescriptionBuilder().setDimensions(dimensions);
		return this;
	}

	/** Exact brute-force (flat) index instead of the default HNSW graph, for representations without their own config. */
	public FieldConfigBuilder flat() {
		vectorIndexingConfigBuilder().flat();
		return this;
	}

	/** Records the embedding model that produces this field's vectors (provenance only). */
	public FieldConfigBuilder model(String modelName) {
		vectorDescriptionBuilder().setModelName(modelName);
		return this;
	}

	public FieldConfigBuilder model(String modelName, String modelDescription) {
		vectorDescriptionBuilder().setModelName(modelName).setModelDescription(modelDescription);
		return this;
	}

	/** Replaces the vector description (dimensions, similarity, model provenance). */
	public FieldConfigBuilder vectorDescription(VectorDescription vectorDescription) {
		this.vectorDescription = vectorDescription.toBuilder();
		return this;
	}

	/**
	 * Indexes this vector field under an additional name with its own encoding, e.g.
	 * {@code createVector("v").dimensions(384).indexAs("vBBQ", Encoding.BBQ).indexAs("vExact", Encoding.FLOAT32)}.
	 */
	public FieldConfigBuilder indexAs(String indexedFieldName, VectorIndexingConfig.Encoding encoding) {
		return indexAs(indexedFieldName, VectorIndexingConfig.newBuilder().setEncoding(encoding).build());
	}

	/** Indexes this vector field under an additional name with its own config, e.g. {@code indexAs("v8", VectorIndexingConfigBuilder.create().quantization(INT8).hnsw(24, 200))}. */
	public FieldConfigBuilder indexAs(String indexedFieldName, VectorIndexingConfigBuilder vectorIndexingConfigBuilder) {
		return indexAs(indexedFieldName, vectorIndexingConfigBuilder.build());
	}

	/** Indexes this vector field under an additional name with its own {@link VectorIndexingConfig}. */
	public FieldConfigBuilder indexAs(String indexedFieldName, VectorIndexingConfig vectorIndexingConfig) {
		return indexAs(IndexAs.newBuilder().setIndexFieldName(indexedFieldName).setVectorIndexingConfig(vectorIndexingConfig).build());
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

	public FieldConfigBuilder facetWithOwnGroup() {
		FacetAs.Builder builder = FacetAs.newBuilder().setFacetName(storedFieldName).setHierarchical(false).setStoreInOwnGroup(true);
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

	public FieldConfigBuilder facetAs(String facetName, String facetGroup) {
		FacetAs.Builder builder = FacetAs.newBuilder().setFacetName(facetName).setHierarchical(false).addFacetGroups(facetGroup);
		return facetAs(builder.build());
	}

	public FieldConfigBuilder facetAsWithOwnGroup(String facetName, String facetGroup) {
		FacetAs.Builder builder = FacetAs.newBuilder().setFacetName(facetName).setHierarchical(false).addFacetGroups(facetGroup).setStoreInOwnGroup(true);
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

	public FieldConfigBuilder facetAs(FacetConfigBuilder facetConfigBuilder) {
		FacetAs.Builder builder = facetConfigBuilder.build();
		if (builder.getFacetName().isEmpty()) {
			builder.setFacetName(storedFieldName);
		}
		this.facetAsList.add(builder.build());
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

	public FieldConfigBuilder sortAs(String sortFieldName) {
		this.sortAsList.add(SortAs.newBuilder().setSortFieldName(sortFieldName).build());
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

	/**
	 * Builds (with {@code true}) or opts out of (with {@code false}) a Lucene doc-values skip index on this field's sort
	 * doc-values - enabling block-skipping range queries and dynamic-pruning sorts. When left unset, the server defaults
	 * it on for new fields. Once a field exists, its flag is frozen (Lucene treats the skip index as immutable schema).
	 */
	public FieldConfigBuilder docValueSkipIndex(boolean docValueSkipIndex) {
		this.docValueSkipIndex = docValueSkipIndex;
		return this;
	}

	public FieldConfig build() {
		FieldConfig.Builder fcBuilder = FieldConfig.newBuilder();
		fcBuilder.setStoredFieldName(storedFieldName);
		fcBuilder.setFieldType(fieldType);
		if (vectorIndexingConfig != null) {
			// the field-wide indexing config is the default for representations that did not set their own
			VectorIndexingConfig defaultIndexingConfig = vectorIndexingConfig.build();
			for (IndexAs indexAs : indexAsList) {
				fcBuilder.addIndexAs(indexAs.hasVectorIndexingConfig() ? indexAs : indexAs.toBuilder().setVectorIndexingConfig(defaultIndexingConfig).build());
			}
		}
		else {
			fcBuilder.addAllIndexAs(indexAsList);
		}
		if (vectorDescription != null) {
			fcBuilder.setVectorDescription(vectorDescription);
		}
		fcBuilder.addAllFacetAs(facetAsList);
		fcBuilder.addAllSortAs(sortAsList);
		if (geoPointConfig != null) {
			fcBuilder.setGeoPointConfig(geoPointConfig);
		}
		if (description != null) {
			fcBuilder.setDescription(description);
		}
		if (displayName != null) {
			fcBuilder.setDisplayName(displayName);
		}
		if (docValueSkipIndex != null) {
			fcBuilder.setDocValueSkipIndex(docValueSkipIndex);
		}
		return fcBuilder.build();
	}

}
