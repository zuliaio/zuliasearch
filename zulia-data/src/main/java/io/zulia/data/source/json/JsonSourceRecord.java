package io.zulia.data.source.json;

import io.zulia.data.source.DataSourceRecord;
import io.zulia.util.BooleanUtil;
import io.zulia.util.document.DocumentHelper;
import org.bson.Document;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
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
		return BooleanUtil.parseBoolean(document.get(field));
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

	/**
	 * A stored date is read as its UTC calendar day, which is how the MongoDB codecs store a LocalDate. Text is read as an ISO local date.
	 */
	@Override
	public LocalDate getLocalDate(String field) {
		return switch (document.get(field)) {
			case null -> null;
			case Date date -> date.toInstant().atOffset(ZoneOffset.UTC).toLocalDate();
			case String text -> LocalDate.parse(text);
			case Object other -> throw notALocal(field, other, "an ISO local date");
		};
	}

	/**
	 * A stored date is read as its UTC wall clock time, which is how the MongoDB codecs store a LocalDateTime. Text is read as an ISO
	 * local date time.
	 */
	@Override
	public LocalDateTime getLocalDateTime(String field) {
		return switch (document.get(field)) {
			case null -> null;
			case Date date -> date.toInstant().atOffset(ZoneOffset.UTC).toLocalDateTime();
			case String text -> LocalDateTime.parse(text);
			case Object other -> throw notALocal(field, other, "an ISO local date time");
		};
	}

	private static IllegalArgumentException notALocal(String field, Object value, String expected) {
		return new IllegalArgumentException("Field <" + field + "> holds " + value.getClass().getSimpleName() + " <" + value + "> and not a date or " + expected);
	}

	public Object getValue(String fullField) {
		return DocumentHelper.getValueFromMongoDocument(document, fullField);
	}

	public Document getAsDocument() {
		return document;
	}
}
