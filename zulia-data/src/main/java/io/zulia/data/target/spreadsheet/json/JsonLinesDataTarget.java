package io.zulia.data.target.spreadsheet.json;

import org.bson.Document;

import java.io.IOException;
import java.io.OutputStreamWriter;

public class JsonLinesDataTarget implements AutoCloseable {

	private final JsonLineDataTargetConfig jsonLineDataTargetConfig;

	private OutputStreamWriter writer;

	public JsonLinesDataTarget(JsonLineDataTargetConfig jsonLineDataTargetConfig) throws IOException {
		this.jsonLineDataTargetConfig = jsonLineDataTargetConfig;
		open();
	}

	protected void open() throws IOException {
		writer = new OutputStreamWriter(jsonLineDataTargetConfig.getDataStream().openOutputStream());
	}

	public void writeRecord(Document document) throws IOException {
		writer.write(document.toJson());
		writer.write(System.lineSeparator());
	}

	public void close() throws IOException {
		writer.close();
	}

}
