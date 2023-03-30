package io.zulia.server.index.field;

import io.zulia.message.ZuliaIndex.FieldConfig.FieldType;
import io.zulia.server.field.FieldTypeUtil;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntPoint;

public class IntFieldIndexer extends NumericFieldIndexer {

	public static final IntFieldIndexer INSTANCE = new IntFieldIndexer();

	protected IntFieldIndexer() {

	}

	@Override
	protected Field createField(Number o, String indexedFieldName) {
		return new IntPoint(FieldTypeUtil.getIndexField(indexedFieldName, FieldType.NUMERIC_INT), o.intValue());
	}

	@Override
	protected Number parseString(String value) {
		return Integer.parseInt(value);
	}

}
