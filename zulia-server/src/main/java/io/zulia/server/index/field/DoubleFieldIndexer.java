package io.zulia.server.index.field;

import io.zulia.message.ZuliaIndex.FieldConfig.FieldType;
import io.zulia.server.field.FieldTypeUtil;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;

public class DoubleFieldIndexer extends NumericFieldIndexer {

	public static final DoubleFieldIndexer INSTANCE = new DoubleFieldIndexer();

	protected DoubleFieldIndexer() {

	}

	@Override
	protected Field createField(Number o, String indexedFieldName) {
		return new DoublePoint(FieldTypeUtil.getIndexField(indexedFieldName, FieldType.NUMERIC_DOUBLE), o.doubleValue());
	}

	@Override
	protected Number parseString(String value) {
		return Double.parseDouble(value);
	}

}
