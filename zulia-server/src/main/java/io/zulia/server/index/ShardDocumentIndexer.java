package io.zulia.server.index;

import com.google.common.base.Splitter;
import com.google.common.primitives.Floats;
import com.koloboke.collect.map.IntObjMap;
import com.koloboke.collect.map.hash.HashIntObjMaps;
import com.koloboke.collect.set.IntSet;
import com.koloboke.collect.set.hash.HashIntSet;
import com.koloboke.collect.set.hash.HashIntSets;
import io.zulia.ZuliaConstants;
import io.zulia.message.ZuliaIndex;
import io.zulia.server.analysis.analyzer.BooleanAnalyzer;
import io.zulia.server.config.ServerIndexConfig;
import io.zulia.server.field.FieldTypeUtil;
import io.zulia.server.index.field.BooleanFieldIndexer;
import io.zulia.server.index.field.DateFieldIndexer;
import io.zulia.server.index.field.DoubleFieldIndexer;
import io.zulia.server.index.field.FloatFieldIndexer;
import io.zulia.server.index.field.IntFieldIndexer;
import io.zulia.server.index.field.LongFieldIndexer;
import io.zulia.server.index.field.StringFieldIndexer;
import io.zulia.util.ResultHelper;
import io.zulia.util.ZuliaUtil;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

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
import java.util.function.IntConsumer;

import static io.zulia.ZuliaConstants.FACET_PATH_DELIMITER;

public class ShardDocumentIndexer {

	private final static Splitter facetPathSplitter = Splitter.on(FACET_PATH_DELIMITER).omitEmptyStrings();

	private final static Map<String, Integer> dimToOrdinal = new ConcurrentHashMap<>();

	private final ServerIndexConfig indexConfig;

	public ShardDocumentIndexer(ServerIndexConfig indexConfig) {
		this.indexConfig = indexConfig;

	}

	public Document getIndexDocument(String uniqueId, long timestamp, DocumentContainer mongoDocument, DocumentContainer metadata,
			DirectoryTaxonomyWriter taxoWriter) throws Exception {
		Document luceneDocument = new Document();

		luceneDocument.add(new StringField(ZuliaConstants.ID_FIELD, uniqueId, Field.Store.YES));
		luceneDocument.add(new SortedSetDocValuesField(indexConfig.getSortField(ZuliaConstants.ID_SORT_FIELD, ZuliaIndex.FieldConfig.FieldType.STRING),
				new BytesRef(uniqueId)));
		luceneDocument.add(new LongPoint(ZuliaConstants.TIMESTAMP_FIELD, timestamp));
		luceneDocument.add(new StoredField(ZuliaConstants.TIMESTAMP_FIELD, timestamp));
		if (!metadata.isEmpty()) {
			luceneDocument.add(new StoredField(ZuliaConstants.STORED_META_FIELD, new BytesRef(metadata.getByteArray())));
		}
		if (!mongoDocument.isEmpty()) {
			luceneDocument.add(new StoredField(ZuliaConstants.STORED_DOC_FIELD, new BytesRef(mongoDocument.getByteArray())));
			addUserFields(mongoDocument.getDocument(), luceneDocument, taxoWriter);
		}

		return luceneDocument;

	}

	private void addUserFields(org.bson.Document mongoDocument, Document luceneDocument, DirectoryTaxonomyWriter taxoWriter) throws Exception {

		Map<String, Set<FacetLabel>> facetFieldToFacetLabels = new HashMap<>();
		for (String storedFieldName : indexConfig.getIndexedStoredFieldNames()) {

			ZuliaIndex.FieldConfig fc = indexConfig.getFieldConfig(storedFieldName);

			if (fc != null) {

				ZuliaIndex.FieldConfig.FieldType fieldType = fc.getFieldType();

				Object o = ResultHelper.getValueFromMongoDocument(mongoDocument, storedFieldName);

				if (o != null) {
					generateFacetLabels(fc, o, facetFieldToFacetLabels);

					addSortForStoredField(luceneDocument, storedFieldName, fc, o);

					addIndexingForStoredField(luceneDocument, storedFieldName, fc, fieldType, o);

				}
			}

		}

		//important that every document has facets (even if empty stored) for intersecting the iterators and doing simultaneous processing of stats and facets stats
		addFacets(luceneDocument, taxoWriter, facetFieldToFacetLabels);
	}

	private static void addFacets(Document luceneDocument, DirectoryTaxonomyWriter taxoWriter, Map<String, Set<FacetLabel>> facetFieldToFacetLabels)
			throws IOException {

		IntObjMap<IntSet> facetDimToOrdinal = HashIntObjMaps.newMutableMap();

		int fieldOrdinalCount = 0;
		for (String facetField : facetFieldToFacetLabels.keySet()) {

			Set<FacetLabel> facetLabels = facetFieldToFacetLabels.get(facetField);

			int dimOridinal = dimToOrdinal.computeIfAbsent(facetField, s -> {
				try {
					return taxoWriter.addCategory(new FacetLabel(facetField));
				}
				catch (IOException e) {
					throw new RuntimeException(e);
				}
			});

			HashIntSet fieldOrdinals = HashIntSets.newMutableSet();
			facetDimToOrdinal.put(dimOridinal, fieldOrdinals);

			for (FacetLabel facetLabel : facetLabels) {

				for (int i = 1; i <= facetLabel.length; i++) {
					luceneDocument.add(
							new StringField(FacetsConfig.DEFAULT_INDEX_FIELD_NAME, FacetsConfig.pathToString(facetLabel.components, i), Field.Store.NO));
				}

				int ordinal = taxoWriter.addCategory(facetLabel);
				fieldOrdinals.add(ordinal);

				int parent = taxoWriter.getParent(ordinal);
				while (parent != dimOridinal && parent > 0) {
					fieldOrdinals.add(parent);
					parent = taxoWriter.getParent(parent);
				}

			}

			fieldOrdinalCount += fieldOrdinals.size();
		}

		TreeSet<Integer> orderedDimOrdinals = new TreeSet<>(facetDimToOrdinal.keySet());

		ByteBuffer byteBuffer = ByteBuffer.allocate(((orderedDimOrdinals.size() * 2) + fieldOrdinalCount) * 4);

		IntBuffer ordinalBuffer = byteBuffer.asIntBuffer();
		for (Integer dimOrdinal : orderedDimOrdinals) {
			IntSet fieldOrdinals = facetDimToOrdinal.get(dimOrdinal);
			ordinalBuffer.put(dimOrdinal);
			ordinalBuffer.put(fieldOrdinals.size());
			fieldOrdinals.forEach((IntConsumer) ordinalBuffer::put);
		}

		luceneDocument.add(new BinaryDocValuesField(ZuliaConstants.FACET_STORAGE, new BytesRef(byteBuffer.array())));
	}

	private void addIndexingForStoredField(Document luceneDocument, String storedFieldName, ZuliaIndex.FieldConfig fc,
			ZuliaIndex.FieldConfig.FieldType fieldType, Object o) throws Exception {
		for (ZuliaIndex.IndexAs indexAs : fc.getIndexAsList()) {

			String indexedFieldName = indexAs.getIndexFieldName();
			luceneDocument.add(new StringField(ZuliaConstants.FIELDS_LIST_FIELD, indexedFieldName, Field.Store.NO));

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
							ZuliaIndex.FieldConfig.FieldType.UNIT_VECTOR.equals(fieldType) ?
									VectorSimilarityFunction.DOT_PRODUCT :
									VectorSimilarityFunction.COSINE));
				}
			}
			else {
				throw new RuntimeException("Unsupported field type <" + fieldType + ">");
			}
		}
	}

	private void addSortForStoredField(Document d, String storedFieldName, ZuliaIndex.FieldConfig fc, Object o) {

		ZuliaIndex.FieldConfig.FieldType fieldType = fc.getFieldType();
		for (ZuliaIndex.SortAs sortAs : fc.getSortAsList()) {

			String sortFieldName = indexConfig.getSortField(sortAs.getSortFieldName(), fieldType);

			if (FieldTypeUtil.isNumericOrDateFieldType(fieldType)) {
				ZuliaUtil.handleListsUniqueValues(o, obj -> {

					if (FieldTypeUtil.isDateFieldType(fieldType)) {
						if (obj instanceof Date date) {
							SortedNumericDocValuesField docValue = new SortedNumericDocValuesField(sortFieldName, date.getTime());
							d.add(docValue);
						}
						else {
							throw new RuntimeException(
									"Expecting date for document field <" + storedFieldName + "> / sort field <" + sortFieldName + ">, found <" + o.getClass()
											+ ">");
						}
					}
					else {
						if (obj instanceof Number number) {

							SortedNumericDocValuesField docValue;
							if (FieldTypeUtil.isNumericIntFieldType(fieldType)) {
								docValue = new SortedNumericDocValuesField(sortFieldName, number.intValue());
							}
							else if (FieldTypeUtil.isNumericLongFieldType(fieldType)) {
								docValue = new SortedNumericDocValuesField(sortFieldName, number.longValue());
							}
							else if (FieldTypeUtil.isNumericFloatFieldType(fieldType)) {
								docValue = new SortedNumericDocValuesField(sortFieldName, NumericUtils.floatToSortableInt(number.floatValue()));
							}
							else if (FieldTypeUtil.isNumericDoubleFieldType(fieldType)) {
								docValue = new SortedNumericDocValuesField(sortFieldName, NumericUtils.doubleToSortableLong(number.doubleValue()));
							}
							else {
								throw new RuntimeException(
										"Not handled numeric field type <" + fieldType + "> for document field <" + storedFieldName + "> / sort field <"
												+ sortFieldName + ">");
							}

							d.add(docValue);
						}
						else {
							throw new RuntimeException(
									"Expecting number for document field <" + storedFieldName + "> / sort field <" + sortFieldName + ">, found <" + o.getClass()
											+ ">");
						}
					}
				});
			}
			else if (ZuliaIndex.FieldConfig.FieldType.BOOL.equals(fieldType)) {

				ZuliaUtil.handleListsUniqueValues(o, obj -> {
					if (obj instanceof Boolean) {
						SortedNumericDocValuesField docValue = new SortedNumericDocValuesField(sortFieldName, (Boolean) obj ? 1 : 0);
						d.add(docValue);
					}
					else if (obj instanceof Number) {
						Number num = (Number) (obj);
						if (num.intValue() == 1) {
							SortedNumericDocValuesField docValue = new SortedNumericDocValuesField(sortFieldName, 1);
							d.add(docValue);
						}
						else if (num.intValue() == 0) {
							SortedNumericDocValuesField docValue = new SortedNumericDocValuesField(sortFieldName, 0);
							d.add(docValue);
						}
					}
					else {
						String string = obj.toString();
						if (BooleanAnalyzer.truePattern.matcher(string).matches()) {
							SortedNumericDocValuesField docValue = new SortedNumericDocValuesField(sortFieldName, 1);
							d.add(docValue);
						}
						else if (BooleanAnalyzer.falsePattern.matcher(string).matches()) {
							SortedNumericDocValuesField docValue = new SortedNumericDocValuesField(sortFieldName, 0);
							d.add(docValue);
						}

					}
				});
			}
			else if (ZuliaIndex.FieldConfig.FieldType.STRING.equals(fieldType)) {
				ZuliaUtil.handleListsUniqueValues(o, obj -> {
					String text = o.toString();

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
									"Not handled string handling <" + stringHandling + "> for document field <" + storedFieldName + "> / sort field <"
											+ sortFieldName + ">");
					}

					if (text.length() > 32766) {
						throw new IllegalArgumentException(
								"Field <" + sortAs.getSortFieldName() + "> is too large to sort.  Must be less <= 32766 characters and is " + text.length());
					}

					SortedSetDocValuesField docValue = new SortedSetDocValuesField(sortFieldName, new BytesRef(text));
					d.add(docValue);
				});
			}
			else {
				throw new RuntimeException(
						"Not handled field type <" + fieldType + "> for document field <" + storedFieldName + "> / sort field <" + sortFieldName + ">");
			}

		}
	}

	private void generateFacetLabels(ZuliaIndex.FieldConfig fc, Object o, Map<String, Set<FacetLabel>> facetFieldToFacetLabels) {
		for (ZuliaIndex.FacetAs fa : fc.getFacetAsList()) {

			String facetName = fa.getFacetName();

			Set<FacetLabel> facetFieldsForField = new TreeSet<>();
			facetFieldToFacetLabels.put(facetName, facetFieldsForField);
			if (fa.getHierarchical()) {

				if (ZuliaIndex.FieldConfig.FieldType.DATE.equals(fc.getFieldType())) {
					ZuliaIndex.FacetAs.DateHandling dateHandling = fa.getDateHandling();
					ZuliaUtil.handleListsUniqueValues(o, obj -> {
						if (obj instanceof Date) {
							LocalDate localDate = ((Date) (obj)).toInstant().atZone(ZoneId.of("UTC")).toLocalDate();

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
						else {
							throw new RuntimeException("Cannot facet date for document field <" + fc.getStoredFieldName() + "> / facet <" + fa.getFacetName()
									+ ">: excepted Date or Collection of Date, found <" + o.getClass().getSimpleName() + ">");
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
				if (ZuliaIndex.FieldConfig.FieldType.DATE.equals(fc.getFieldType())) {
					ZuliaIndex.FacetAs.DateHandling dateHandling = fa.getDateHandling();
					ZuliaUtil.handleListsUniqueValues(o, obj -> {
						if (obj instanceof Date) {
							LocalDate localDate = ((Date) (obj)).toInstant().atZone(ZoneId.of("UTC")).toLocalDate();

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
						else {
							throw new RuntimeException("Cannot facet date for document field <" + fc.getStoredFieldName() + "> / facet <" + fa.getFacetName()
									+ ">: excepted Date or Collection of Date, found <" + o.getClass().getSimpleName() + ">");
						}
					});
				}
				else if (ZuliaIndex.FieldConfig.FieldType.BOOL.equals(fc.getFieldType())) {
					ZuliaUtil.handleListsUniqueValues(o, obj -> {
						String string = obj.toString();

						if (BooleanAnalyzer.truePattern.matcher(string).matches()) {
							facetFieldsForField.add(new FacetLabel(facetName, "True"));
						}
						else if (BooleanAnalyzer.falsePattern.matcher(string).matches()) {
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
