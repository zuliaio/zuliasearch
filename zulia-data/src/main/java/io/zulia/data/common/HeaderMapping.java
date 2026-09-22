package io.zulia.data.common;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.SequencedSet;

public class HeaderMapping {

	private final List<String> headers;
	private final HeaderConfig headerConfig;

	private final LinkedHashMap<String, Integer> headersMap;

	public HeaderMapping(HeaderConfig headerConfig, List<String> headerRow) {
		List<String> headers = headerConfig.isIgnoreTrailingBlanks() ? withoutTrailingBlanks(headerRow) : headerRow;
		// read only for every source type
		this.headers = Collections.unmodifiableList(headers);
		this.headerConfig = headerConfig;
		this.headersMap = new LinkedHashMap<>();

		if (headers.isEmpty()) {
			throw new IllegalArgumentException("Headers are set but spreadsheet contains an empty header");
		}
		for (int i = 0; i < headers.size(); i++) {
			String header = headers.get(i);

			header = (header == null) ? "" : header.trim();

			if (header.isEmpty() && !headerConfig.isAllowBlanks()) {
				throw new IllegalArgumentException("Header contains an empty cell and allow blanks is not set");
			}

			if (headersMap.containsKey(header)) {
				if (!headerConfig.isAllowDuplicates()) {
					throw new IllegalArgumentException("Header contains duplicate headers and allow duplicates is not set");
				}
				int count = 2;
				while (headersMap.containsKey(header + "_" + count)) {
					count++;
				}
				header = header + "_" + count;
			}
			headersMap.put(header, i);
		}
	}

	private static List<String> withoutTrailingBlanks(List<String> headers) {
		int end = headers.size();
		while (end > 0 && isBlank(headers.get(end - 1))) {
			end--;
		}
		return headers.subList(0, end);
	}

	private static boolean isBlank(String header) {
		return header == null || header.isBlank();
	}

	public boolean hasHeader(String field) {
		return headersMap.containsKey(field);
	}

	public SequencedSet<String> getHeaderKeys() {
		return headersMap.sequencedKeySet();
	}

	public List<String> getRawHeaders() {
		return headers;
	}

	public int getHeaderIndex(String field) {
		return headersMap.getOrDefault(field, -1);
	}

	public HeaderConfig getHeaderConfig() {
		return headerConfig;
	}

}
