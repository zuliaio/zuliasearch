package io.zulia.data.source.spreadsheet;

import io.zulia.data.common.DataStreamMeta;
import io.zulia.data.common.SpreadsheetType;
import io.zulia.data.input.DataInputStream;
import io.zulia.data.input.FileDataInputStream;
import io.zulia.data.input.SingleUseDataInputStream;
import io.zulia.data.source.spreadsheet.csv.CSVSource;
import io.zulia.data.source.spreadsheet.csv.CSVSourceConfig;
import io.zulia.data.source.spreadsheet.excel.ExcelSource;
import io.zulia.data.source.spreadsheet.excel.ExcelSourceConfig;
import io.zulia.data.source.spreadsheet.tsv.TSVSource;
import io.zulia.data.source.spreadsheet.tsv.TSVSourceConfig;

import java.io.IOException;
import java.io.InputStream;
import java.util.Objects;
import java.util.function.Consumer;

public class SpreadsheetSourceFactory {

	public enum HeaderOptions {
		STRICT,
		STANDARD,
		NONE
	}

	public static SpreadsheetSource<?> fromFileWithoutHeaders(String filePath) throws IOException {
		return fromStream(FileDataInputStream.from(filePath), HeaderOptions.NONE);
	}

	public static SpreadsheetSource<?> fromFileWithHeaders(String filePath) throws IOException {
		return fromStream(FileDataInputStream.from(filePath), HeaderOptions.STANDARD);
	}

	public static SpreadsheetSource<?> fromFileWithStrictHeaders(String filePath) throws IOException {
		return fromStream(FileDataInputStream.from(filePath), HeaderOptions.STRICT);
	}

	public static SpreadsheetSource<?> fromFile(String filePath, HeaderOptions headerOptions) throws IOException {
		return fromStream(FileDataInputStream.from(filePath), headerOptions);
	}

	/**
	 * Same as {@link #fromFile(String, HeaderOptions)} with the configurer applied to the source's config.
	 */
	public static SpreadsheetSource<?> fromFile(String filePath, HeaderOptions headerOptions, Consumer<SpreadsheetSourceConfig> configurer) throws IOException {
		return fromStream(FileDataInputStream.from(filePath), headerOptions, configurer);
	}

	public static SpreadsheetSource<?> fromStreamWithoutHeaders(DataInputStream dataInputStream) throws IOException {
		return fromStream(dataInputStream, HeaderOptions.NONE);
	}

	public static SpreadsheetSource<?> fromStreamWithHeaders(DataInputStream dataInputStream) throws IOException {
		return fromStream(dataInputStream, HeaderOptions.STANDARD);
	}

	public static SpreadsheetSource<?> fromStreamWithStrictHeaders(DataInputStream dataInputStream) throws IOException {
		return fromStream(dataInputStream, HeaderOptions.STRICT);
	}

	public static SpreadsheetSource<?> fromSingleUseStreamWithoutHeaders(InputStream inputStream, DataStreamMeta dataStreamMeta) throws IOException {
		return fromStream(SingleUseDataInputStream.from(inputStream, dataStreamMeta), HeaderOptions.NONE);
	}

	public static SpreadsheetSource<?> fromSingleUseStreamWithHeaders(InputStream inputStream, DataStreamMeta dataStreamMeta) throws IOException {
		return fromStream(SingleUseDataInputStream.from(inputStream, dataStreamMeta), HeaderOptions.STANDARD);
	}

	public static SpreadsheetSource<?> fromSingleUseStreamWithStrictHeaders(InputStream inputStream, DataStreamMeta dataStreamMeta) throws IOException {
		return fromStream(SingleUseDataInputStream.from(inputStream, dataStreamMeta), HeaderOptions.STRICT);
	}

	public static SpreadsheetSource<?> fromSingleUseStream(InputStream inputStream, DataStreamMeta dataStreamMeta, HeaderOptions headerOptions) throws IOException {
		return fromStream(SingleUseDataInputStream.from(inputStream, dataStreamMeta), headerOptions);
	}

	/**
	 * Same as {@link #fromSingleUseStream(InputStream, DataStreamMeta, HeaderOptions)} with the configurer applied to the source's config.
	 */
	public static SpreadsheetSource<?> fromSingleUseStream(InputStream inputStream, DataStreamMeta dataStreamMeta, HeaderOptions headerOptions,
			Consumer<SpreadsheetSourceConfig> configurer) throws IOException {
		return fromStream(SingleUseDataInputStream.from(inputStream, dataStreamMeta), headerOptions, configurer);
	}

	public static SpreadsheetSource<?> fromStream(DataInputStream dataInputStream, HeaderOptions headerOptions) throws IOException {
		return fromStream(dataInputStream, headerOptions, null);
	}

	/**
	 * Creates the source for the stream's spreadsheet type. The header options are applied first and then the configurer, so a
	 * caller can set the cell parsers, the list delimiter or a list handler without knowing which type-specific config was built.
	 * A null configurer keeps the config's defaults.
	 */
	public static SpreadsheetSource<?> fromStream(DataInputStream dataInputStream, HeaderOptions headerOptions, Consumer<SpreadsheetSourceConfig> configurer)
			throws IOException {
		Objects.requireNonNull(headerOptions, "headerOptions");
		SpreadsheetType spreadsheetType = SpreadsheetType.getSpreadsheetType(dataInputStream.getMeta());
		if (spreadsheetType == null) {
			throw new IllegalArgumentException(
					"Failed to determine file type from content type <" + dataInputStream.getMeta().contentType() + "> with filename <"
							+ dataInputStream.getMeta().fileName() + ">");
		}
		return switch (spreadsheetType) {
			case CSV -> CSVSource.withConfig(configure(CSVSourceConfig.from(dataInputStream), headerOptions, configurer));
			case TSV -> TSVSource.withConfig(configure(TSVSourceConfig.from(dataInputStream), headerOptions, configurer));
			case XLSX, XLS -> ExcelSource.withConfig(configure(ExcelSourceConfig.from(dataInputStream), headerOptions, configurer));
		};
	}

	private static <C extends SpreadsheetSourceConfig> C configure(C config, HeaderOptions headerOptions, Consumer<SpreadsheetSourceConfig> configurer) {
		switch (headerOptions) {
			case STRICT -> config.withStrictHeaders();
			case STANDARD -> config.withHeaders();
			case NONE -> config.withoutHeaders();
		}
		if (configurer != null) {
			configurer.accept(config);
		}
		return config;
	}

}
