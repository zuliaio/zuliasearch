package io.zulia.data.target.spreadsheet.json;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class GsonObjectSerializer implements ObjectSerializer {
	private final Gson gson;

	public GsonObjectSerializer() {
		gson = new GsonBuilder().create();
	}

	@Override
	public String getObjectAsJson(Object o) {
		return gson.toJson(o);
	}
}
