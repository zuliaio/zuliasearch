package io.zulia.server.index;

import com.google.common.base.Splitter;
import io.zulia.ZuliaFieldConstants;
import io.zulia.message.ZuliaBase;
import io.zulia.message.ZuliaIndex;
import io.zulia.message.ZuliaIndex.FieldConfig;
import io.zulia.server.config.ServerIndexConfig;
import io.zulia.server.field.FieldDefault;
import io.zulia.server.field.FieldTypeUtil;
import io.zulia.server.index.field.BooleanFieldIndexer;
import io.zulia.server.index.field.DateFieldIndexer;
import io.zulia.server.index.field.DoubleFieldIndexer;
import io.zulia.server.index.field.FloatFieldIndexer;
import io.zulia.server.index.field.IntFieldIndexer;
import io.zulia.server.index.field.LongFieldIndexer;
import io.zulia.server.index.field.StoredFieldHandler;
import io.zulia.server.index.field.StoredFieldValueResolver;
import io.zulia.server.index.field.StringFieldIndexer;
import io.zulia.util.LatLon;
import io.zulia.util.ZuliaVersion;
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
import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;
import org.xerial.snappy.Snappy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.time.LocalDate;
import java.time.ZoneId;
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
			boolean nothingConsumesTheValue = fc.getIndexAsCount() == 0 && fc.getFacetAsCount() == 0 && fc.getSortAsCount() == 0;
			if (nothingConsumesTheValue) {
				// a type-only config must never reject a document
				continue;
			}
			String storedFieldName = fc.getStoredFieldName();
			FieldConfig.FieldType fieldType = fc.getFieldType();

			FieldDefault fieldDefault = indexConfig.getFieldDefault(storedFieldName);
			StoredFieldHandler handler = StoredFieldValueResolver.resolve(mongoDocument, fc, fieldDefault);
			if (handler.isPresent()) {
				addMarkers(luceneDocument, fc, handler);
				generateFacetLabels(fc, handler, facetFieldToFacetLabels);
				addSortForStoredField(luceneDocument, storedFieldName, fc, handler);
				addIndexingForStoredField(luceneDocument, storedFieldName, fc, fieldType, handler);
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

	private static void addMarkers(Document luceneDocument, FieldConfig fc, StoredFieldHandler handler) {
		if (handler.existsMarker()) {
			for (ZuliaIndex.IndexAs indexAs : fc.getIndexAsList()) {
				luceneDocument.add(new StringField(ZuliaFieldConstants.FIELDS_LIST_FIELD, indexAs.getIndexFieldName(), Field.Store.NO));
			}
		}
		if (handler.hasMalformedValues()) {
			luceneDocument.add(new StringField(ZuliaFieldConstants.MALFORMED_FIELDS_LIST_FIELD, malformedMarkerName(fc), Field.Store.NO));
			luceneDocument.add(new StringField(ZuliaFieldConstants.FIELDS_LIST_FIELD, ZuliaFieldConstants.MALFORMED_FIELDS_LIST_FIELD, Field.Store.NO));
		}
	}

	/**
	 * A geo point reading top-level coordinate keys has no stored field name, so the name it is indexed or sorted under stands in.
	 */
	private static String malformedMarkerName(FieldConfig fc) {
		if (!fc.getStoredFieldName().isEmpty()) {
			return fc.getStoredFieldName();
		}
		if (fc.getIndexAsCount() > 0) {
			return fc.getIndexAs(0).getIndexFieldName();
		}
		return fc.getSortAsCount() > 0 ? fc.getSortAs(0).getSortFieldName() : fc.getStoredFieldName();
	}

	private void addIndexingForStoredField(Document luceneDocument, String storedFieldName, FieldConfig fc, FieldConfig.FieldType fieldType,
			StoredFieldHandler handler) {
		for (ZuliaIndex.IndexAs indexAs : fc.getIndexAsList()) {

			String indexedFieldName = indexAs.getIndexFieldName();

			if (FieldTypeUtil.isNumericIntFieldType(fieldType)) {
				IntFieldIndexer.INSTANCE.index(luceneDocument, storedFieldName, handler, indexedFieldName);
			}
			else if (FieldTypeUtil.isNumericLongFieldType(fieldType)) {
				LongFieldIndexer.INSTANCE.index(luceneDocument, storedFieldName, handler, indexedFieldName);
			}
			else if (FieldTypeUtil.isNumericFloatFieldType(fieldType)) {
				FloatFieldIndexer.INSTANCE.index(luceneDocument, storedFieldName, handler, indexedFieldName);
			}
			else if (FieldTypeUtil.isNumericDoubleFieldType(fieldType)) {
				DoubleFieldIndexer.INSTANCE.index(luceneDocument, storedFieldName, handler, indexedFieldName);
			}
			else if (FieldTypeUtil.isDateFieldType(fieldType)) {
				DateFieldIndexer.INSTANCE.index(luceneDocument, storedFieldName, handler, indexedFieldName);
			}
			else if (FieldTypeUtil.isBooleanFieldType(fieldType)) {
				BooleanFieldIndexer.INSTANCE.index(luceneDocument, storedFieldName, handler, indexedFieldName);
			}
			else if (FieldTypeUtil.isStringFieldType(fieldType)) {
				StringFieldIndexer.INSTANCE.index(luceneDocument, storedFieldName, handler, indexedFieldName);
			}
			else if (FieldTypeUtil.isVectorFieldType(fieldType)) {
				// Quantization is purely a codec concern. The indexed field stays a plain KnnFloatVectorField.
				ZuliaIndex.VectorDescription vectorDescription = fc.getVectorDescription();
				VectorSimilarityFunction similarity = VectorFieldResolver.resolveSimilarity(vectorDescription, fieldType);
				handler.onAllValues(obj -> {
					if (obj instanceof float[] vector) {
						luceneDocument.add(new KnnFloatVectorField(indexedFieldName, vector, similarity));
					}
				});
			}
			else if (FieldTypeUtil.isGeoPointFieldType(fieldType)) {
				String indexField = FieldTypeUtil.getIndexField(indexedFieldName, fieldType);
				handler.onAllValues(obj -> {
					if (obj instanceof LatLon(double latitude, double longitude)) {
						luceneDocument.add(new LatLonPoint(indexField, latitude, longitude));
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

	private void addSortForStoredField(Document d, String storedFieldName, FieldConfig fc, StoredFieldHandler handler) {

		FieldConfig.FieldType fieldType = fc.getFieldType();

		// When enabled, sort doc-values are written with a Lucene skip index so range queries block-skip and sorts can dynamically prune.
		boolean docValueSkipIndex = fc.getDocValueSkipIndex();

		if (FieldTypeUtil.isGeoPointFieldType(fieldType)) {
			if (fc.getSortAsCount() > 0) {
				// the last point of a multi-valued field is the one sorted on. This reads every value
				// rather than just the last, because a point the type read as absent stays in place and is not one
				LatLon[] last = { null };
				handler.onAllValues(obj -> {
					if (obj instanceof LatLon point) {
						last[0] = point;
					}
				});
				if (last[0] != null) {
					for (ZuliaIndex.SortAs sortAs : fc.getSortAsList()) {
						String sortFieldName = FieldTypeUtil.getSortField(sortAs.getSortFieldName(), fieldType);
						d.add(new LatLonDocValuesField(sortFieldName, last[0].latitude(), last[0].longitude()));
					}
				}
			}
			return;
		}

		for (ZuliaIndex.SortAs sortAs : fc.getSortAsList()) {

			String sortFieldName = FieldTypeUtil.getSortField(sortAs.getSortFieldName(), fieldType);

			if (FieldTypeUtil.isStringFieldType(fieldType)) {
				handler.onUniqueValues(obj -> {
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
				handler.onUniqueValues(obj -> {
					// the resolver already parsed numeric strings, so anything else here is a value it read as absent
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
							throw new RuntimeException("Not handled numeric field type <" + fieldType + "> for sort field <" + sortAs.getSortFieldName() + ">");
						}

						d.add(docValue);
					}
				});
			}
			else if (FieldTypeUtil.isBooleanFieldType(fieldType)) {
				handler.onUniqueValues(obj -> {
					if (obj instanceof Boolean boolVal) {
						d.add(numericSortField(sortFieldName, boolVal ? 1 : 0, docValueSkipIndex));
					}
				});
			}
			else if (FieldTypeUtil.isDateFieldType(fieldType)) {
				handler.onUniqueValues(obj -> {
					if (obj instanceof Date date) {
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

	private void generateFacetLabels(FieldConfig fc, StoredFieldHandler handler, Map<String, Set<FacetLabel>> facetFieldToFacetLabels) {
		for (ZuliaIndex.FacetAs fa : fc.getFacetAsList()) {

			String facetName = fa.getFacetName();

			Set<FacetLabel> facetFieldsForField = new TreeSet<>();
			facetFieldToFacetLabels.put(facetName, facetFieldsForField);
			if (fa.getHierarchical()) {

				if (FieldTypeUtil.isDateFieldType(fc.getFieldType())) {
					ZuliaIndex.FacetAs.DateHandling dateHandling = fa.getDateHandling();
					handler.onUniqueValues(obj -> {
						if (obj instanceof Date date) {
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
				else if (FieldConfig.FieldType.BOOL.equals(fc.getFieldType())) {
					// the same label as the flat facet, rather than the raw stored text
					handler.onUniqueValues(obj -> {
						if (obj instanceof Boolean boolVal) {
							facetFieldsForField.add(new FacetLabel(facetName, boolVal ? "True" : "False"));
						}
					});
				}
				else {
					handler.onUniqueValues(obj -> {
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
					handler.onUniqueValues(obj -> {
						if (obj instanceof Date dateValue) {
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
					handler.onUniqueValues(obj -> {
						if (obj instanceof Boolean boolVal) {
							facetFieldsForField.add(new FacetLabel(facetName, boolVal ? "True" : "False"));
						}
					});
				}
				else if (FieldTypeUtil.isNumericIntFieldType(fc.getFieldType()) || FieldTypeUtil.isNumericLongFieldType(fc.getFieldType())) {
					handler.onUniqueValues(obj -> {
						String val = integralFacetLabel(obj, fc.getFieldType());
						if (!val.isEmpty()) {
							facetFieldsForField.add(new FacetLabel(facetName, val));
						}
					});
				}
				else {
					handler.onUniqueValues(obj -> {
						String val = obj.toString();
						if (!val.isEmpty()) {
							facetFieldsForField.add(new FacetLabel(facetName, val));
						}
					});
				}

			}

		}
	}

	private static String integralFacetLabel(Object obj, FieldConfig.FieldType fieldType) {
		if (obj instanceof Number number) {
			if (FieldTypeUtil.isNumericIntFieldType(fieldType)) {
				return Integer.toString(number.intValue());
			}
			if (FieldTypeUtil.isNumericLongFieldType(fieldType)) {
				return Long.toString(number.longValue());
			}
		}
		return obj.toString();
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
