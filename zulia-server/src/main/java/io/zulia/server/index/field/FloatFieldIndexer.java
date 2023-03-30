package io.zulia.server.index.field;

import io.zulia.message.ZuliaIndex.FieldConfig.FieldType;
import io.zulia.server.field.FieldTypeUtil;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatPoint;

public class FloatFieldIndexer extends NumericFieldIndexer {

	public static final FloatFieldIndexer INSTANCE = new FloatFieldIndexer();

	protected FloatFieldIndexer() {

	}

	@Override
	protected Field createField(Number o, String indexedFieldName) {
		return new FloatPoint(FieldTypeUtil.getIndexField(indexedFieldName, FieldType.NUMERIC_FLOAT), o.floatValue());
	}

	@Override
	protected Number parseString(String value) {
		return Float.parseFloat(value);
	}

}
