package io.zulia.server.index.field;

import io.zulia.message.ZuliaIndex.FieldConfig.FieldType;
import io.zulia.server.field.FieldTypeUtil;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.LongPoint;

import java.util.Date;

public class DateFieldIndexer extends FieldIndexer {

	public static final DateFieldIndexer INSTANCE = new DateFieldIndexer();

	protected DateFieldIndexer() {

	}

	@Override
	protected void handleValue(Document d, String storedFieldName, Object value, String indexedFieldName) {
		// the resolver already converted these, and anything it read as absent keeps its place without being one
		if (value instanceof Date date) {
			d.add(createField(date, indexedFieldName));
		}
	}

	protected Field createField(Date o, String indexedFieldName) {
		return new LongPoint(FieldTypeUtil.getIndexField(indexedFieldName, FieldType.DATE), o.getTime());
	}

}
