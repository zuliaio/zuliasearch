package io.zulia.data.test;

import io.zulia.data.input.SingleUseDataInputStream;
import io.zulia.data.source.json.JsonArraySource;
import io.zulia.data.source.json.JsonSourceRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class JsonArraySourceTest {

	@Test
	public void parsesMultipleObjectsFromArray() throws Exception {
		String json = """
				[
				  {"id": "a", "count": 1, "ratio": 1.5, "active": true},
				  {"id": "b", "count": 2, "ratio": 2.5, "active": false},
				  {"id": "c", "count": 3, "ratio": 3.5, "active": true}
				]
				""";

		List<JsonSourceRecord> records = readAll(json);

		Assertions.assertEquals(3, records.size());

		Assertions.assertEquals(List.of("a", "b", "c"), records.stream().map(r -> r.getString("id")).toList());
		Assertions.assertEquals(1, records.getFirst().getInt("count"));
		Assertions.assertEquals(1.5, records.getFirst().getDouble("ratio"));
		Assertions.assertEquals(Boolean.TRUE, records.getFirst().getBoolean("active"));
		Assertions.assertEquals(Boolean.FALSE, records.get(1).getBoolean("active"));
	}

	@Test
	public void parsesEmptyArray() throws Exception {
		Assertions.assertTrue(readAll("[]").isEmpty());
	}

	private static List<JsonSourceRecord> readAll(String json) throws Exception {
		var dataInputStream = SingleUseDataInputStream.from(new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)), "test.json");
		List<JsonSourceRecord> records = new ArrayList<>();
		try (JsonArraySource source = JsonArraySource.withDefaults(dataInputStream)) {
			for (JsonSourceRecord record : source) {
				records.add(record);
			}
		}
		return records;
	}
}
