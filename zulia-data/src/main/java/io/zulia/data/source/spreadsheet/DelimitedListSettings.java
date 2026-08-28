package io.zulia.data.source.spreadsheet;

import java.util.Objects;

/**
 * The delimited list configuration shared by the delimited and excel source configs: the list delimiter, the cell parsers and
 * the list handler. The handler is either one set explicitly or the default handler built from the delimiter and parsers.
 * Setting the delimiter drops an explicit handler, as the source configs always did, while a parser change keeps it.
 */
public final class DelimitedListSettings {

	private char listDelimiter = ';';
	private CellParsers parsers = CellParsers.defaults();
	private DelimitedListHandler explicitHandler;
	private DefaultDelimitedListHandler builtHandler;

	public DelimitedListSettings withListDelimiter(char listDelimiter) {
		this.listDelimiter = listDelimiter;
		this.explicitHandler = null;
		this.builtHandler = null;
		return this;
	}

	public DelimitedListSettings withHandler(DelimitedListHandler handler) {
		this.explicitHandler = Objects.requireNonNull(handler, "handler");
		return this;
	}

	public DelimitedListSettings withParsers(CellParsers parsers) {
		this.parsers = Objects.requireNonNull(parsers, "parsers");
		this.builtHandler = null;
		return this;
	}

	public CellParsers getParsers() {
		return parsers;
	}

	public DelimitedListHandler getHandler() {
		if (explicitHandler != null) {
			return explicitHandler;
		}
		if (builtHandler == null) {
			builtHandler = new DefaultDelimitedListHandler(listDelimiter, parsers);
		}
		return builtHandler;
	}
}
