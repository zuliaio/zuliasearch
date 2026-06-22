package io.zulia.server.index;

import com.google.common.base.Splitter;
import com.google.common.primitives.Floats;
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;
import io.zulia.ZuliaFieldConstants;
import io.zulia.message.ZuliaBase;
import io.zulia.message.ZuliaIndex;
import io.zulia.message.ZuliaIndex.FieldConfig;
import io.zulia.server.config.ServerIndexConfig;
import io.zulia.server.field.FieldTypeUtil;
import io.zulia.server.index.field.BooleanFieldIndexer;
import io.zulia.server.index.field.DateFieldIndexer;
import io.zulia.server.index.field.DoubleFieldIndexer;
import io.zulia.server.index.field.FloatFieldIndexer;
import io.zulia.server.index.field.IntFieldIndexer;
import io.zulia.server.index.field.LongFieldIndexer;
import io.zulia.server.index.field.StringFieldIndexer;
import io.zulia.util.BooleanUtil;
import io.zulia.util.ZuliaDateUtil;
import io.zulia.util.ZuliaUtil;
import io.zulia.util.ZuliaVersion;
import io.zulia.util.document.DocumentHelper;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.document.LatLonPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

public class ShardDocumentIndexer {

	private final static Splitter facetPathSplitter = Splitter.on(ZuliaFieldConstants.FACET_PATH_DELIMITER).omitEmptyStrings();

	private final ServerIndexConfig indexConfig;
	private final int majorVersion;
	private final int minorVersion;
	private final String idSortField;
	private final Map<String, Integer> dimOrdinalCache = new ConcurrentHashMap<>();

	public ShardDocumentIndexer(ServerIndexConfig indexConfig) {
		this.indexConfig = indexConfig;
		this.majorVersion = ZuliaVersion.getMajor();
		this.minorVersion = ZuliaVersion.getMinor();
		this.idSortField = FieldTypeUtil.getSortField(ZuliaFieldConstants.ID_SORT_FIELD, FieldConfig.FieldType.STRING);
	}

	public Document getIndexDocument(String uniqueId, long timestamp, DocumentContainer mongoDocument, DocumentContainer metadata,
			DirectoryTaxonomyWriter taxoWriter) throws Exception {
		Document luceneDocument = new Document();
		luceneDocument.add(new StringField(ZuliaFieldConstants.ID_FIELD, uniqueId, Field.Store.NO));
		// The built-in id sort field is not a FieldConfig, so its skip index is gated on the index-creation version: new indexes
		// get it, indexes created before the feature (version 0) keep the plain field so their immutable schema is unchanged.
		boolean idSortDocValueSkipIndex = indexConfig.getIndexSettings().getCreatedIndexVersion() >= ZuliaIndexVersion.ID_SORT_DOC_VALUE_SKIP;
		luceneDocument.add(sortedSetSortField(idSortField, new BytesRef(uniqueId), idSortDocValueSkipIndex));
		luceneDocument.add(new LongPoint(ZuliaFieldConstants.TIMESTAMP_FIELD, timestamp));

		boolean compressionEnabled = indexConfig.isCompressionEnabled();
		ZuliaBase.IdInfo idInfo = ZuliaBase.IdInfo.newBuilder().setId(uniqueId).setTimestamp(timestamp).setMajorVersion(majorVersion)
				.setMinorVersion(minorVersion).setCompressedDoc(compressionEnabled).build();

		byte[] idInfoBytes = idInfo.toByteArray();

		luceneDocument.add(new BinaryDocValuesField(ZuliaFieldConstants.STORED_ID_FIELD, new BytesRef(idInfoBytes)));

		if (metadata.hasDocument()) {
			byte[] bytes = compressionEnabled ? Snappy.compress(metadata.getByteArray()) : metadata.getByteArray();
			luceneDocument.add(new BinaryDocValuesField(ZuliaFieldConstants.STORED_META_FIELD, new BytesRef(bytes)));
		}
		if (mongoDocument.hasDocument()) {
			byte[] bytes = compressionEnabled ? Snappy.compress(mongoDocument.getByteArray()) : mongoDocument.getByteArray();
			luceneDocument.add(new BinaryDocValuesField(ZuliaFieldConstants.STORED_DOC_FIELD, new BytesRef(bytes)));
			addUserFields(mongoDocument.getDocument(), luceneDocument, taxoWriter);
		}

		return luceneDocument;

	}

	private void addUserFields(org.bson.Document mongoDocument, Document luceneDocument, DirectoryTaxonomyWriter taxoWriter) throws Exception {

		Map<String, Set<FacetLabel>> facetFieldToFacetLabels = new HashMap<>();
		for (FieldConfig fc : indexConfig.getIndexSettings().getFieldConfigList()) {
			String storedFieldName = fc.getStoredFieldName();
			FieldConfig.FieldType fieldType = fc.getFieldType();

			Object o = DocumentHelper.getValueFromMongoDocument(mongoDocument, storedFieldName);
			if (o == null && FieldTypeUtil.isGeoPointFieldType(fieldType) && storedFieldName.isEmpty()) {
				// For GEO_POINT with empty storedFieldName, lat/lon keys are at top level
				o = mongoDocument;
			}
			if (o != null) {
				generateFacetLabels(fc, o, facetFieldToFacetLabels);
				addSortForStoredField(luceneDocument, storedFieldName, fc, o);
				addIndexingForStoredField(luceneDocument, storedFieldName, fc, fieldType, o);
			}

		}

		//important that every document has facets (even if empty stored) for intersecting the iterators and doing simultaneous processing of stats and facets stats
		addFacets(luceneDocument, taxoWriter, facetFieldToFacetLabels);
	}

	private void addFacets(Document luceneDocument, DirectoryTaxonomyWriter taxoWriter, Map<String, Set<FacetLabel>> facetFieldToFacetLabels)
			throws IOException {

		IntObjectHashMap<IntHashSet> facetDimToOrdinal = new IntObjectHashMap<>();

		int fieldOrdinalCount = 0;
		for (String facetField : facetFieldToFacetLabels.keySet()) {

			Set<FacetLabel> facetLabels = facetFieldToFacetLabels.get(facetField);

			int dimOridinal = getOrdinalForFacetField(taxoWriter, facetField);

			IntHashSet fieldOrdinals = new IntHashSet();
			facetDimToOrdinal.put(dimOridinal, fieldOrdinals);

			for (FacetLabel facetLabel : facetLabels) {

				for (int i = 1; i <= facetLabel.length; i++) {
					luceneDocument.add(
							new StringField(ZuliaFieldConstants.FACET_DRILL_DOWN_FIELD, FacetsConfig.pathToString(facetLabel.components, i), Field.Store.NO));
				}

				int ordinal = taxoWriter.addCategory(facetLabel);
				fieldOrdinals.add(ordinal);

				int parent = taxoWriter.getParent(ordinal);
				while (parent != dimOridinal && parent > 0) {
					fieldOrdinals.add(parent);
					parent = taxoWriter.getParent(parent);
				}

			}
			if (indexConfig.isStoredIndividually(facetField)) {
				storeIndividualFacets(luceneDocument, facetField, fieldOrdinals);
			}

			fieldOrdinalCount += fieldOrdinals.size();
		}

		Map<String, Set<String>> facetGroupToFacets = indexConfig.getFacetGroups();
		for (String facetGroup : facetGroupToFacets.keySet()) {
			Set<String> facetsInGroup = facetGroupToFacets.get(facetGroup);
			IntHashSet dimOrdinalsForGroup = new IntHashSet();
			for (String facet : facetsInGroup) {
				dimOrdinalsForGroup.add(getOrdinalForFacetField(taxoWriter, facet));
			}
			storeOrderedFacetsAsDocValue(luceneDocument, dimOrdinalsForGroup.toSortedArray(), fieldOrdinalCount, facetDimToOrdinal,
					ZuliaFieldConstants.FACET_STORAGE_GROUP + facetGroup);
		}

		int[] orderedDimOrdinals = facetDimToOrdinal.keysView().toSortedArray();
		storeOrderedFacetsAsDocValue(luceneDocument, orderedDimOrdinals, fieldOrdinalCount, facetDimToOrdinal, ZuliaFieldConstants.FACET_STORAGE);
	}

	private int getOrdinalForFacetField(DirectoryTaxonomyWriter taxoWriter, String facetField) throws IOException {
		Integer cached = dimOrdinalCache.get(facetField);
		if (cached != null) {
			return cached;
		}
		int ordinal = taxoWriter.addCategory(new FacetLabel(facetField));
		dimOrdinalCache.put(facetField, ordinal);
		return ordinal;
	}

	private static void storeIndividualFacets(Document luceneDocument, String facetField, IntHashSet fieldOrdinals) {
		ByteBuffer byteBuffer = ByteBuffer.allocate((fieldOrdinals.size() + 1) * 4);
		IntBuffer ordinalBuffer = byteBuffer.asIntBuffer();
		ordinalBuffer.put(fieldOrdinals.size());
		fieldOrdinals.forEach(ordinalBuffer::put);
		luceneDocument.add(new BinaryDocValuesField(ZuliaFieldConstants.FACET_STORAGE_INDIVIDUAL + facetField, new BytesRef(byteBuffer.array())));
	}

	private static void storeOrderedFacetsAsDocValue(Document luceneDocument, int[] sortedDimOrdinals, int fieldOrdinalCount,
			IntObjectHashMap<IntHashSet> facetDimToOrdinal, String field) {
		ByteBuffer byteBuffer = ByteBuffer.allocate(((sortedDimOrdinals.length * 2) + fieldOrdinalCount) * 4);

		IntBuffer ordinalBuffer = byteBuffer.asIntBuffer();
		for (int dimOrdinal : sortedDimOrdinals) {
			IntHashSet fieldOrdinals = facetDimToOrdinal.get(dimOrdinal);
			ordinalBuffer.put(dimOrdinal);
			if (fieldOrdinals != null) {
				ordinalBuffer.put(fieldOrdinals.size());
				fieldOrdinals.forEach(ordinalBuffer::put);
			}
			else {
				ordinalBuffer.put(0);
			}
		}

		luceneDocument.add(new BinaryDocValuesField(field, new BytesRef(byteBuffer.array())));
	}

	private void addIndexingForStoredField(Document luceneDocument, String storedFieldName, FieldConfig fc, FieldConfig.FieldType fieldType, Object o) {
		for (ZuliaIndex.IndexAs indexAs : fc.getIndexAsList()) {

			String indexedFieldName = indexAs.getIndexFieldName();
			luceneDocument.add(new StringField(ZuliaFieldConstants.FIELDS_LIST_FIELD, indexedFieldName, Field.Store.NO));

			if (FieldTypeUtil.isNumericIntFieldType(fieldType)) {
				IntFieldIndexer.INSTANCE.index(luceneDocument, storedFieldName, o, indexedFieldName);
			}
			else if (FieldTypeUtil.isNumericLongFieldType(fieldType)) {
				LongFieldIndexer.INSTANCE.index(luceneDocument, storedFieldName, o, indexedFieldName);
			}
			else if (FieldTypeUtil.isNumericFloatFieldType(fieldType)) {
				FloatFieldIndexer.INSTANCE.index(luceneDocument, storedFieldName, o, indexedFieldName);
			}
			else if (FieldTypeUtil.isNumericDoubleFieldType(fieldType)) {
				DoubleFieldIndexer.INSTANCE.index(luceneDocument, storedFieldName, o, indexedFieldName);
			}
			else if (FieldTypeUtil.isDateFieldType(fieldType)) {
				DateFieldIndexer.INSTANCE.index(luceneDocument, storedFieldName, o, indexedFieldName);
			}
			else if (FieldTypeUtil.isBooleanFieldType(fieldType)) {
				BooleanFieldIndexer.INSTANCE.index(luceneDocument, storedFieldName, o, indexedFieldName);
			}
			else if (FieldTypeUtil.isStringFieldType(fieldType)) {
				StringFieldIndexer.INSTANCE.index(luceneDocument, storedFieldName, o, indexedFieldName);
			}
			else if (FieldTypeUtil.isVectorFieldType(fieldType)) {
				if (o instanceof Collection collection) {
					luceneDocument.add(new KnnFloatVectorField(indexedFieldName, Floats.toArray(collection),
							FieldConfig.FieldType.UNIT_VECTOR.equals(fieldType) ? VectorSimilarityFunction.DOT_PRODUCT : VectorSimilarityFunction.COSINE));
				}
			}
			else if (FieldTypeUtil.isGeoPointFieldType(fieldType)) {
				String indexField = FieldTypeUtil.getIndexField(indexedFieldName, fieldType);
				ZuliaIndex.GeoPointConfig gpc = fc.getGeoPointConfig();
				String latKey = gpc.getLatitudeKey().isEmpty() ? "latitude" : gpc.getLatitudeKey();
				String lonKey = gpc.getLongitudeKey().isEmpty() ? "longitude" : gpc.getLongitudeKey();

				ZuliaUtil.handleLists(o, obj -> {
					if (obj instanceof org.bson.Document geoDoc) {
						Double lat = null;
						Double lon = null;
						if ("Point".equals(geoDoc.get("type"))) {
							// GeoJSON: coordinates = [longitude, latitude]
							Object coordsObj = geoDoc.get("coordinates");
							if (coordsObj instanceof List<?> coords && coords.size() >= 2) {
								lon = ((Number) coords.get(0)).doubleValue();
								lat = ((Number) coords.get(1)).doubleValue();
							}
						}
						else {
							Number latNum = geoDoc.get(latKey, Number.class);
							Number lonNum = geoDoc.get(lonKey, Number.class);
							if (latNum != null) {
								lat = latNum.doubleValue();
							}
							if (lonNum != null) {
								lon = lonNum.doubleValue();
							}
						}
						if (lat != null && lon != null) {
							luceneDocument.add(new LatLonPoint(indexField, lat, lon));
						}
					}
				});
			}
			else {
				throw new RuntimeException("Unsupported field type <" + fieldType + ">");
			}
		}
	}

	private static SortedNumericDocValuesField numericSortField(String name, long value, boolean docValueSkipIndex) {
		return docValueSkipIndex ? SortedNumericDocValuesField.indexedField(name, value) : new SortedNumericDocValuesField(name, value);
	}

	private static SortedSetDocValuesField sortedSetSortField(String name, BytesRef value, boolean docValueSkipIndex) {
		return docValueSkipIndex ? SortedSetDocValuesField.indexedField(name, value) : new SortedSetDocValuesField(name, value);
	}

	private void addSortForStoredField(Document d, String storedFieldName, FieldConfig fc, Object o) {

		FieldConfig.FieldType fieldType = fc.getFieldType();

		// When enabled, sort doc-values are written with a Lucene skip index so range queries block-skip and sorts can dynamically prune.
		boolean docValueSkipIndex = fc.getDocValueSkipIndex();

		if (FieldTypeUtil.isGeoPointFieldType(fieldType)) {
			if (fc.getSortAsCount() > 0) {
				ZuliaIndex.GeoPointConfig gpc = fc.getGeoPointConfig();
				String latKey = gpc.getLatitudeKey().isEmpty() ? "latitude" : gpc.getLatitudeKey();
				String lonKey = gpc.getLongitudeKey().isEmpty() ? "longitude" : gpc.getLongitudeKey();

				double[] lastLatLon = { Double.NaN, Double.NaN };
				ZuliaUtil.handleLists(o, obj -> {
					if (obj instanceof org.bson.Document geoDoc) {
						Double lat = null;
						Double lon = null;
						if ("Point".equals(geoDoc.get("type"))) {
							Object coordsObj = geoDoc.get("coordinates");
							if (coordsObj instanceof List<?> coords && coords.size() >= 2) {
								lon = ((Number) coords.get(0)).doubleValue();
								lat = ((Number) coords.get(1)).doubleValue();
							}
						}
						else {
							Number latNum = geoDoc.get(latKey, Number.class);
							Number lonNum = geoDoc.get(lonKey, Number.class);
							if (latNum != null) {
								lat = latNum.doubleValue();
							}
							if (lonNum != null) {
								lon = lonNum.doubleValue();
							}
						}
						if (lat != null && lon != null) {
							lastLatLon[0] = lat;
							lastLatLon[1] = lon;
						}
					}
				});
				if (!Double.isNaN(lastLatLon[0])) {
					for (ZuliaIndex.SortAs sortAs : fc.getSortAsList()) {
						String sortFieldName = FieldTypeUtil.getSortField(sortAs.getSortFieldName(), fieldType);
						d.add(new LatLonDocValuesField(sortFieldName, lastLatLon[0], lastLatLon[1]));
					}
				}
			}
			return;
		}

		for (ZuliaIndex.SortAs sortAs : fc.getSortAsList()) {

			String sortFieldName = FieldTypeUtil.getSortField(sortAs.getSortFieldName(), fieldType);

			if (FieldTypeUtil.isStringFieldType(fieldType)) {
				ZuliaUtil.handleListsUniqueValues(o, obj -> {
					String text = obj.toString();

					ZuliaIndex.SortAs.StringHandling stringHandling = sortAs.getStringHandling();
					switch (stringHandling) {
						case STANDARD:
							break;
						case LOWERCASE:
							text = text.toLowerCase();
							break;
						case FOLDING:
							text = getFoldedString(text);
							break;
						case LOWERCASE_FOLDING:
							text = getFoldedString(text).toLowerCase();
							break;
						default:
							throw new RuntimeException(
									"Not handled string handling " + stringHandling + " for document field " + storedFieldName + " / sort field "
											+ sortFieldName);
					}

					if (text.length() > 32766) {
						throw new IllegalArgumentException(
								"Field " + sortAs.getSortFieldName() + " is too large to sort.  Must be less <= 32766 characters and is " + text.length());
					}

					SortedSetDocValuesField docValue = sortedSetSortField(sortFieldName, new BytesRef(text), docValueSkipIndex);
					d.add(docValue);
				});
			}
			else if (FieldTypeUtil.isNumericFieldType(fieldType)) {
				ZuliaUtil.handleListsUniqueValues(o, obj -> {
					if (obj instanceof Number number) {

						SortedNumericDocValuesField docValue;
						if (FieldTypeUtil.isNumericIntFieldType(fieldType)) {
							docValue = numericSortField(sortFieldName, number.intValue(), docValueSkipIndex);
						}
						else if (FieldTypeUtil.isNumericLongFieldType(fieldType)) {
							docValue = numericSortField(sortFieldName, number.longValue(), docValueSkipIndex);
						}
						else if (FieldTypeUtil.isNumericFloatFieldType(fieldType)) {
							docValue = numericSortField(sortFieldName, NumericUtils.floatToSortableInt(number.floatValue()), docValueSkipIndex);
						}
						else if (FieldTypeUtil.isNumericDoubleFieldType(fieldType)) {
							docValue = numericSortField(sortFieldName, NumericUtils.doubleToSortableLong(number.doubleValue()), docValueSkipIndex);
						}
						else {
							throw new RuntimeException(
									"Not handled numeric field type <" + fieldType + "> for sort field <" + sortAs.getSortFieldName() + "> from number");
						}

						d.add(docValue);
					}
					else if (obj instanceof String value) {
						SortedNumericDocValuesField docValue;
						try {
							if (FieldTypeUtil.isNumericIntFieldType(fieldType)) {
								docValue = numericSortField(sortFieldName, Integer.parseInt(value), docValueSkipIndex);
							}
							else if (FieldTypeUtil.isNumericLongFieldType(fieldType)) {
								docValue = numericSortField(sortFieldName, Long.parseLong(value), docValueSkipIndex);
							}
							else if (FieldTypeUtil.isNumericFloatFieldType(fieldType)) {
								docValue = numericSortField(sortFieldName, NumericUtils.floatToSortableInt(Float.parseFloat(value)), docValueSkipIndex);
							}
							else if (FieldTypeUtil.isNumericDoubleFieldType(fieldType)) {
								docValue = numericSortField(sortFieldName, NumericUtils.doubleToSortableLong(Double.parseDouble(value)), docValueSkipIndex);
							}
							else {
								throw new RuntimeException(
										"Not handled numeric field type <" + fieldType + "> for sort field <" + sortAs.getSortFieldName() + "> from string");
							}
							d.add(docValue);
						}
						catch (NumberFormatException e) {
							throw new RuntimeException(
									"String value <" + value + "> for field <" + sortAs.getSortFieldName() + "> cannot be parsed as numeric type <" + fieldType
											+ ">");
						}
					}
					else {
						throw new RuntimeException(
								"Expecting number for document field <" + storedFieldName + "> / sort field <" + sortFieldName + ">, found <" + o.getClass()
										+ ">");
					}
				});
			}
			else if (FieldTypeUtil.isBooleanFieldType(fieldType)) {
				ZuliaUtil.handleListsUniqueValues(o, obj -> {
					if (obj instanceof Boolean) {
						SortedNumericDocValuesField docValue = numericSortField(sortFieldName, (Boolean) obj ? 1 : 0, docValueSkipIndex);
						d.add(docValue);
					}
					else if (obj instanceof Number) {
						Number num = (Number) (obj);
						if (num.intValue() == 1) {
							SortedNumericDocValuesField docValue = numericSortField(sortFieldName, 1, docValueSkipIndex);
							d.add(docValue);
						}
						else if (num.intValue() == 0) {
							SortedNumericDocValuesField docValue = numericSortField(sortFieldName, 0, docValueSkipIndex);
							d.add(docValue);
						}
					}
					else {
						String string = obj.toString();
						int booleanInt = BooleanUtil.getStringAsBooleanInt(string);
						if (booleanInt >= 0) {
							SortedNumericDocValuesField docValue = numericSortField(sortFieldName, booleanInt, docValueSkipIndex);
							d.add(docValue);
						}
					}
				});
			}
			else if (FieldTypeUtil.isDateFieldType(fieldType)) {
				ZuliaUtil.handleListsUniqueValues(o, obj -> {
					Date date = ZuliaDateUtil.convertToDate(obj, "document field <" + storedFieldName + "> / sort field <" + sortFieldName + ">");
					if (date != null) {
						d.add(numericSortField(sortFieldName, date.getTime(), docValueSkipIndex));
					}
				});
			}
			else {
				throw new RuntimeException(
						"Not handled field type <" + fieldType + "> for document field <" + storedFieldName + "> / sort field <" + sortFieldName + ">");
			}

		}
	}

	private void generateFacetLabels(FieldConfig fc, Object o, Map<String, Set<FacetLabel>> facetFieldToFacetLabels) {
		for (ZuliaIndex.FacetAs fa : fc.getFacetAsList()) {

			String facetName = fa.getFacetName();

			Set<FacetLabel> facetFieldsForField = new TreeSet<>();
			facetFieldToFacetLabels.put(facetName, facetFieldsForField);
			if (fa.getHierarchical()) {

				if (FieldTypeUtil.isDateFieldType(fc.getFieldType())) {
					ZuliaIndex.FacetAs.DateHandling dateHandling = fa.getDateHandling();
					ZuliaUtil.handleListsUniqueValues(o, obj -> {
						Date date = ZuliaDateUtil.convertToDate(obj,
								"document field <" + fc.getStoredFieldName() + "> / facet <" + fa.getFacetName() + ">");
						if (date != null) {
							LocalDate localDate = date.toInstant().atZone(ZoneId.of("UTC")).toLocalDate();

							if (ZuliaIndex.FacetAs.DateHandling.DATE_YYYYMMDD.equals(dateHandling)) {
								facetFieldsForField.add(
										new FacetLabel(facetName, localDate.getYear() + "", localDate.getMonthValue() + "", localDate.getDayOfMonth() + ""));

							}
							else if (ZuliaIndex.FacetAs.DateHandling.DATE_YYYY_MM_DD.equals(dateHandling)) {
								facetFieldsForField.add(
										new FacetLabel(facetName, localDate.getYear() + "", localDate.getMonthValue() + "", localDate.getDayOfMonth() + ""));

							}
							else {
								throw new RuntimeException("Not handled date handling <" + dateHandling + "> for facet <" + fa.getFacetName() + ">");
							}
						}
					});
				}
				else {
					ZuliaUtil.handleListsUniqueValues(o, obj -> {
						String val = obj.toString();
						if (!val.isEmpty()) {
							List<String> path = facetPathSplitter.splitToList(val);
							facetFieldsForField.add(new FacetLabel(facetName, path.toArray(new String[0])));
						}
					});
				}

			}
			else {
				if (FieldConfig.FieldType.DATE.equals(fc.getFieldType())) {
					ZuliaIndex.FacetAs.DateHandling dateHandling = fa.getDateHandling();
					ZuliaUtil.handleListsUniqueValues(o, obj -> {
						Date dateValue = ZuliaDateUtil.convertToDate(obj,
								"document field <" + fc.getStoredFieldName() + "> / facet <" + fa.getFacetName() + ">");
						if (dateValue != null) {
							LocalDate localDate = dateValue.toInstant().atZone(ZoneId.of("UTC")).toLocalDate();

							if (ZuliaIndex.FacetAs.DateHandling.DATE_YYYYMMDD.equals(dateHandling)) {
								String date = String.format("%02d%02d%02d", localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth());
								facetFieldsForField.add(new FacetLabel(facetName, date));

							}
							else if (ZuliaIndex.FacetAs.DateHandling.DATE_YYYY_MM_DD.equals(dateHandling)) {
								String date = String.format("%02d-%02d-%02d", localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth());
								facetFieldsForField.add(new FacetLabel(facetName, date));

							}
							else {
								throw new RuntimeException("Not handled date handling <" + dateHandling + "> for facet <" + fa.getFacetName() + ">");
							}
						}
					});
				}
				else if (FieldConfig.FieldType.BOOL.equals(fc.getFieldType())) {
					ZuliaUtil.handleListsUniqueValues(o, obj -> {
						String string = obj.toString();

						int booleanInt = BooleanUtil.getStringAsBooleanInt(string);
						if (booleanInt == 1) {
							facetFieldsForField.add(new FacetLabel(facetName, "True"));
						}
						else if (booleanInt == 0) {
							facetFieldsForField.add(new FacetLabel(facetName, "False"));
						}

					});
				}
				else {
					ZuliaUtil.handleListsUniqueValues(o, obj -> {
						String val = obj.toString();
						if (!val.isEmpty()) {
							facetFieldsForField.add(new FacetLabel(facetName, val));
						}
					});
				}

			}

		}
	}

	private static String getFoldedString(String text) {

		boolean needsFolding = false;
		for (int pos = 0; pos < text.length(); ++pos) {
			final char c = text.charAt(pos);

			if (c >= '\u0080') {
				needsFolding = true;
				break;
			}
		}

		if (!needsFolding) {
			return text;
		}

		char[] textChar = text.toCharArray();
		char[] output = new char[textChar.length * 4];
		int outputPos = ASCIIFoldingFilter.foldToASCII(textChar, 0, output, 0, textChar.length);
		text = new String(output, 0, outputPos);
		return text;
	}
}
