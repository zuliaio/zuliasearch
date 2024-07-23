package io.zulia.data.common;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.SequencedSet;

public class HeaderMapping {

	private final List<String> headers;
	private final HeaderConfig headerConfig;

	private final LinkedHashMap<String, Integer> headersMap;

	public HeaderMapping(HeaderConfig headerConfig, List<String> headers) {
		this.headers = headers;
		this.headerConfig = headerConfig;
		this.headersMap = new LinkedHashMap<>();

		if (headers.isEmpty()) {
			throw new IllegalArgumentException("Headers are set but spreadsheet contains an empty header");
		}
		for (int i = 0; i < headers.size(); i++) {
			String header = headers.get(i);
			header = header.trim();

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
