package io.zulia.data.source.json;

import io.zulia.data.source.DataSourceRecord;
import io.zulia.util.document.DocumentHelper;
import org.bson.Document;

import java.util.Date;
import java.util.List;

public class JsonSourceRecord implements DataSourceRecord {

	private final Document document;

	public JsonSourceRecord(String json) {
		document = Document.parse(json);
	}

	@Override
	public <T> List<T> getList(String field, Class<T> clazz) {
		return document.getList(field, clazz);
	}

	@Override
	public String getString(String field) {
		return document.getString(field);
	}

	@Override
	public Boolean getBoolean(String field) {
		return document.getBoolean(field);
	}

	@Override
	public Float getFloat(String field) {
		return DocumentHelper.getAsFloat(document, field);
	}

	@Override
	public Double getDouble(String field) {
		return DocumentHelper.getAsDouble(document, field);
	}

	@Override
	public Integer getInt(String field) {
		return DocumentHelper.getAsInt(document, field);
	}

	@Override
	public Long getLong(String field) {
		return DocumentHelper.getAsLong(document, field);
	}

	@Override
	public Date getDate(String field) {
		return document.getDate(field);
	}

	public Object getValue(String fullField) {
		return DocumentHelper.getValueFromMongoDocument(document, fullField);
	}

	public Document getAsDocument() {
		return document;
	}
}
