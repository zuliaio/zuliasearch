package io.zulia.server.index;

import com.google.common.base.Splitter;
import info.debatty.java.lsh.SuperBit;
import io.zulia.ZuliaConstants;
import io.zulia.message.ZuliaIndex;
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
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.NumericUtils;

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;

import static io.zulia.ZuliaConstants.FACET_PATH_DELIMITER;

public class ShardDocumentIndexer {

	private static Splitter facetPathSplitter = Splitter.on(FACET_PATH_DELIMITER).omitEmptyStrings();

	private final ServerIndexConfig indexConfig;

	public ShardDocumentIndexer(ServerIndexConfig indexConfig) {
		this.indexConfig = indexConfig;

	}

	public Document getIndexDocument(String uniqueId, long timestamp, org.bson.Document mongoDocument, org.bson.Document metadata) throws Exception {
		Document luceneDocument = new Document();

		addStoredFieldsForDocument(mongoDocument, luceneDocument);

		luceneDocument.add(new StringField(ZuliaConstants.ID_FIELD, uniqueId, Field.Store.YES));

		luceneDocument.add(new LongPoint(ZuliaConstants.TIMESTAMP_FIELD, timestamp));
		luceneDocument.add(new StoredField(ZuliaConstants.TIMESTAMP_FIELD, timestamp));

		luceneDocument.add(new StoredField(ZuliaConstants.STORED_DOC_FIELD, new BytesRef(ZuliaUtil.mongoDocumentToByteArray(mongoDocument))));

		luceneDocument.add(new StoredField(ZuliaConstants.STORED_META_FIELD, new BytesRef(ZuliaUtil.mongoDocumentToByteArray(metadata))));

		return luceneDocument;

	}

	private void addStoredFieldsForDocument(org.bson.Document mongoDocument, Document luceneDocument) throws Exception {
		for (String storedFieldName : indexConfig.getIndexedStoredFieldNames()) {

			ZuliaIndex.FieldConfig fc = indexConfig.getFieldConfig(storedFieldName);

			if (fc != null) {

				ZuliaIndex.FieldConfig.FieldType fieldType = fc.getFieldType();

				Object o = ResultHelper.getValueFromMongoDocument(mongoDocument, storedFieldName);

				if (o != null) {
					handleFacetsForStoredField(luceneDocument, fc, o);

					handleSortForStoredField(luceneDocument, storedFieldName, fc, o);

					handleIndexingForStoredField(luceneDocument, storedFieldName, fc, fieldType, o);

					handleProjectForStoredField(luceneDocument, fc, o);
				}
			}

		}
	}

	private void handleProjectForStoredField(Document luceneDocument, ZuliaIndex.FieldConfig fc, Object o) throws Exception {
		for (ZuliaIndex.ProjectAs projectAs : fc.getProjectAsList()) {
			if (projectAs.hasSuperbit()) {
				if (o instanceof List) {
					List<Number> values = (List<Number>) o;

					double vec[] = new double[values.size()];
					int i = 0;
					for (Number value : values) {
						vec[i++] = value.doubleValue();
					}

					SuperBit superBitForField = indexConfig.getSuperBitForField(projectAs.getField());
					boolean[] signature = superBitForField.signature(vec);

					int j = 0;
					for (boolean s : signature) {
						StringFieldIndexer.INSTANCE.index(luceneDocument, projectAs.getField(), s ? "1" : "0",
								ZuliaConstants.SUPERBIT_PREFIX + "." + projectAs.getField() + "." + j);
						j++;
					}

				}
				else {
					throw new Exception("Expecting a list for superbit field <" + projectAs.getField() + ">");
				}
			}
		}
	}

	private void handleIndexingForStoredField(Document luceneDocument, String storedFieldName, ZuliaIndex.FieldConfig fc,
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
			else {
				throw new RuntimeException("Unsupported field type <" + fieldType + ">");
			}
		}
	}

	private void handleSortForStoredField(Document d, String storedFieldName, ZuliaIndex.FieldConfig fc, Object o) {

		ZuliaIndex.FieldConfig.FieldType fieldType = fc.getFieldType();
		for (ZuliaIndex.SortAs sortAs : fc.getSortAsList()) {
			String sortFieldName = sortAs.getSortFieldName();

			if (FieldTypeUtil.isNumericOrDateFieldType(fieldType)) {
				ZuliaUtil.handleListsUniqueValues(o, obj -> {

					if (FieldTypeUtil.isDateFieldType(fieldType)) {
						if (obj instanceof Date) {

							Date date = (Date) obj;
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
						if (obj instanceof Number) {

							Number number = (Number) obj;
							SortedNumericDocValuesField docValue = null;
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
						String text = obj.toString();
						SortedSetDocValuesField docValue = new SortedSetDocValuesField(sortFieldName, new BytesRef(text));
						d.add(docValue);
					}
					else {
						throw new RuntimeException(
								"Expecting boolean for document field <" + storedFieldName + "> / sort field <" + sortFieldName + ">, found <" + o.getClass()
										+ ">");
					}
				});
			}
			else if (ZuliaIndex.FieldConfig.FieldType.STRING.equals(fieldType)) {
				ZuliaUtil.handleListsUniqueValues(o, obj -> {
					String text = o.toString();

					ZuliaIndex.SortAs.StringHandling stringHandling = sortAs.getStringHandling();
					if (ZuliaIndex.SortAs.StringHandling.STANDARD.equals(stringHandling)) {
						//no op
					}
					else if (ZuliaIndex.SortAs.StringHandling.LOWERCASE.equals(stringHandling)) {
						text = text.toLowerCase();
					}
					else if (ZuliaIndex.SortAs.StringHandling.FOLDING.equals(stringHandling)) {
						text = getFoldedString(text);
					}
					else if (ZuliaIndex.SortAs.StringHandling.LOWERCASE_FOLDING.equals(stringHandling)) {
						text = getFoldedString(text).toLowerCase();
					}
					else {
						throw new RuntimeException(
								"Not handled string handling <" + stringHandling + "> for document field <" + storedFieldName + "> / sort field <"
										+ sortFieldName + ">");
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

	private void handleFacetsForStoredField(Document doc, ZuliaIndex.FieldConfig fc, Object o) throws Exception {
		for (ZuliaIndex.FacetAs fa : fc.getFacetAsList()) {

			String facetName = fa.getFacetName();

			if (ZuliaIndex.FieldConfig.FieldType.DATE.equals(fc.getFieldType())) {
				ZuliaIndex.FacetAs.DateHandling dateHandling = fa.getDateHandling();
				ZuliaUtil.handleListsUniqueValues(o, obj -> {
					if (obj instanceof Date) {
						LocalDate localDate = ((Date) (obj)).toInstant().atZone(ZoneId.of("UTC")).toLocalDate();

						if (ZuliaIndex.FacetAs.DateHandling.DATE_YYYYMMDD.equals(dateHandling)) {
							if (fa.getHierarchical()) {
								doc.add(new FacetField(facetName, localDate.getYear() + "", localDate.getMonthValue() + "", localDate.getDayOfMonth() + ""));
							}
							else {
								String date = String.format("%02d%02d%02d", localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth());
								addFacet(doc, facetName, date);
							}
						}
						else if (ZuliaIndex.FacetAs.DateHandling.DATE_YYYY_MM_DD.equals(dateHandling)) {
							if (fa.getHierarchical()) {
								doc.add(new FacetField(facetName, localDate.getYear() + "", localDate.getMonthValue() + "", localDate.getDayOfMonth() + ""));
							}
							else {
								String date = String.format("%02d-%02d-%02d", localDate.getYear(), localDate.getMonthValue(), localDate.getDayOfMonth());
								addFacet(doc, facetName, date);
							}
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
					String string = obj.toString();
					if (!string.isEmpty()) {
						if (fa.getHierarchical()) {
							List<String> path = facetPathSplitter.splitToList(string);
							doc.add(new FacetField(facetName, path.toArray(new String[0])));
						}
						else {
							doc.add(new FacetField(facetName, string));
						}
					}
				});
			}

		}
	}

	private void addFacet(Document doc, String facetName, String value) {
		if (!value.isEmpty()) {
			doc.add(new FacetField(facetName, value));
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
