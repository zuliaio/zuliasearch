package io.zulia.server.index.field;

import io.zulia.server.analysis.analyzer.BooleanAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;

public class BooleanFieldIndexer extends FieldIndexer {

	public static final BooleanFieldIndexer INSTANCE = new BooleanFieldIndexer();
	private static final FieldType notStoredTextField;

	static {
		notStoredTextField = new FieldType(TextField.TYPE_NOT_STORED);
		notStoredTextField.setStoreTermVectors(true);
		notStoredTextField.setStoreTermVectorOffsets(true);
		notStoredTextField.setStoreTermVectorPositions(true);
		// For PostingsHighlighter in Lucene 4.1 +
		// notStoredTextField.setIndexOptions(FieldInfo.IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS);
		// example: https://svn.apache.org/repos/asf/lucene/dev/trunk/lucene/highlighter/src/test/org/apache/lucene/search/postingshighlight/TestPostingsHighlighter.java
		notStoredTextField.freeze();
	}

	protected BooleanFieldIndexer() {

	}

	@Override
	protected void handleValue(Document d, String storedFieldName, Object value, String indexedFieldName) throws Exception {
		if (value != null) {
			if (value instanceof Boolean) {
                d.add((new Field(indexedFieldName, value.toString(), notStoredTextField)));
            } else if (value instanceof String v) {
                if (BooleanAnalyzer.truePattern.matcher(v).matches()) {
                    d.add((new Field(indexedFieldName, BooleanAnalyzer.TRUE_TOKEN, notStoredTextField)));
                } else if (BooleanAnalyzer.falsePattern.matcher(v).matches()) {
                    d.add((new Field(indexedFieldName, BooleanAnalyzer.FALSE_TOKEN, notStoredTextField)));
                } else {
                    throw new Exception(
                            "String for Boolean field be 'Yes', 'No', 'Y', 'N', '1', '0', 'True', 'False', 'T', 'F' (case insensitive) for <" + storedFieldName
                                    + "> and found <" + v + ">");
                }
            } else if (value instanceof Number number) {
                int v = number.intValue();
                if (v == 0 || v == 1) {
                    d.add((new Field(indexedFieldName, String.valueOf(v), notStoredTextField)));
                } else {
                    throw new Exception("Number for Boolean field must be 0 or 1 for <" + storedFieldName + "> and found <" + v + ">");
                }
            } else {
                throw new Exception(
						"Expecting collection of data type of Boolean, String, or Number for field <" + storedFieldName + "> and found <" + value.getClass()
								.getSimpleName() + ">");

			}
		}
	}

}
