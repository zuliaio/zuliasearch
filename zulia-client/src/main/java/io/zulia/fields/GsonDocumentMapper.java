package io.zulia.fields;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import io.zulia.client.result.BatchFetchResult;
import io.zulia.client.result.FetchResult;
import io.zulia.message.ZuliaQuery;
import io.zulia.util.ResultHelper;
import org.bson.Document;

import java.lang.reflect.Type;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Date;
import java.util.List;

public class GsonDocumentMapper<T> {

	private static final DateTimeFormatter MONGO_UTC_FORMAT = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd'T'HH:mm:ss").optionalStart()
			.appendFraction(ChronoField.MILLI_OF_SECOND, 1, 3, true).optionalEnd().appendPattern("'Z'").toFormatter();

	private final Class<T> clazz;
	private final Gson gson = new GsonBuilder().registerTypeAdapter(Date.class, new DateSerializer()).registerTypeAdapter(Date.class, new DateDeserializer())
			.create();

	private static class DateSerializer implements JsonSerializer<Date> {

		@Override
		public JsonElement serialize(Date src, Type typeOfSrc, JsonSerializationContext context) {
			String dateString = MONGO_UTC_FORMAT.format(src.toInstant().atZone(ZoneId.of("UTC")).toLocalDateTime());
			JsonObject jo = new JsonObject();
			jo.addProperty("$date", dateString);
			return jo;
		}
	}

	private static class DateDeserializer implements JsonDeserializer<Date> {

		@Override
		public Date deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
			JsonElement date = json.getAsJsonObject().get("$date");
			if (date.isJsonObject()) {
				Instant instant = Instant.ofEpochSecond(date.getAsJsonObject().get("$numberLong").getAsLong());
				return Date.from(instant);
			}
			else {
				String dateString = date.getAsString();
				return Date.from(LocalDateTime.from(MONGO_UTC_FORMAT.parse(dateString)).toInstant(ZoneOffset.UTC));
			}
		}
	}

	public GsonDocumentMapper(Class<T> clazz) {
		this.clazz = clazz;
	}

	public Document toDocument(T object) {
		String json = gson.toJson(object);
		return Document.parse(json);
	}

	public T fromDocument(Document savedDocument) {
		if (savedDocument != null) {
			String json = savedDocument.toJson();
			return gson.fromJson(json, clazz);
		}
		return null;
	}

	public T fromScoredResult(ZuliaQuery.ScoredResult scoredResult) {
		return fromDocument(ResultHelper.getDocumentFromScoredResult(scoredResult));
	}

	public List<T> fromBatchFetchResult(BatchFetchResult batchFetchResult) throws Exception {
		return batchFetchResult.getDocuments(this);
	}

	public T fromFetchResult(FetchResult fetchResult) throws Exception {
		return fetchResult.getDocument(this);
	}

}
