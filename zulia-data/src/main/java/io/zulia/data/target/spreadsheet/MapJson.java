package io.zulia.data.target.spreadsheet;

import org.bson.Document;

import java.util.Map;

/**
 * Writes a Map as relaxed extended JSON, the form {@code Document.parse} reads back. A key that is not a String is written as
 * its toString.
 */
public final class MapJson {

	private MapJson() {
	}

	public static String toJson(Map<?, ?> map) {
		if (map instanceof Document document) {
			return document.toJson();
		}
		Document document = new Document();
		map.forEach((key, value) -> document.put(String.valueOf(key), value));
		return document.toJson();
	}
}
