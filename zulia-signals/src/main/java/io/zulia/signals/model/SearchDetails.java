package io.zulia.signals.model;

import java.time.Duration;
import java.util.List;

/**
 * Every field optional. searchSignalId names one search signal, searchId is the app's own key across executions and pages, shownDocIds are stored
 * only.
 */
public record SearchDetails(String query, List<String> indexes, Long resultCount, Long latencyMs, Integer clickedPosition, String clickedDocId,
		String searchSignalId, String searchId, List<String> shownDocIds) {

	public static final int MAX_SHOWN_DOC_IDS = 20;

	public SearchDetails {
		indexes = indexes == null ? List.of() : List.copyOf(indexes);
		shownDocIds = shownDocIds == null ? List.of() : List.copyOf(shownDocIds);
		if (shownDocIds.size() > MAX_SHOWN_DOC_IDS) {
			throw new IllegalArgumentException("Shown doc ids are capped at " + MAX_SHOWN_DOC_IDS + ", got " + shownDocIds.size());
		}
		if (resultCount != null && resultCount < 0) {
			throw new IllegalArgumentException("Result count must be zero or more, got " + resultCount);
		}
		if (latencyMs != null && latencyMs < 0) {
			throw new IllegalArgumentException("Latency must be zero or more, got " + latencyMs + " ms");
		}
		if (clickedPosition != null && clickedPosition < 0) {
			throw new IllegalArgumentException("Clicked position must be zero or more, got " + clickedPosition);
		}
		if (searchId != null && searchId.isBlank()) {
			throw new IllegalArgumentException("Search id must not be blank, leave it unset when the app has no key for the search");
		}
	}

	public static Builder builder() {
		return new Builder();
	}

	public Builder toBuilder() {
		Builder builder = new Builder();
		builder.query = query;
		builder.indexes = indexes;
		builder.resultCount = resultCount;
		builder.latencyMs = latencyMs;
		builder.clickedPosition = clickedPosition;
		builder.clickedDocId = clickedDocId;
		builder.searchSignalId = searchSignalId;
		builder.searchId = searchId;
		builder.shownDocIds = shownDocIds;
		return builder;
	}

	public static final class Builder {

		private String query;
		private List<String> indexes;
		private Long resultCount;
		private Long latencyMs;
		private Integer clickedPosition;
		private String clickedDocId;
		private String searchSignalId;
		private String searchId;
		private List<String> shownDocIds;

		private Builder() {
		}

		public Builder query(String query) {
			this.query = query;
			return this;
		}

		public Builder indexes(List<String> indexes) {
			this.indexes = indexes;
			return this;
		}

		public Builder indexes(String... indexes) {
			return indexes(List.of(indexes));
		}

		public Builder resultCount(long resultCount) {
			this.resultCount = resultCount;
			return this;
		}

		public Builder latency(Duration latency) {
			if (latency == null) {
				throw new IllegalArgumentException("Latency must not be null, skip the call to leave it unset");
			}
			this.latencyMs = latency.toMillis();
			return this;
		}

		public Builder latencyMs(long latencyMs) {
			this.latencyMs = latencyMs;
			return this;
		}

		public Builder clickedPosition(int clickedPosition) {
			this.clickedPosition = clickedPosition;
			return this;
		}

		public Builder clickedDocId(String clickedDocId) {
			this.clickedDocId = clickedDocId;
			return this;
		}

		public Builder searchSignalId(String searchSignalId) {
			this.searchSignalId = searchSignalId;
			return this;
		}

		public Builder searchId(String searchId) {
			this.searchId = searchId;
			return this;
		}

		/** Rank order, at most {@link #MAX_SHOWN_DOC_IDS}. */
		public Builder shownDocIds(List<String> shownDocIds) {
			this.shownDocIds = shownDocIds;
			return this;
		}

		public SearchDetails build() {
			return new SearchDetails(query, indexes, resultCount, latencyMs, clickedPosition, clickedDocId, searchSignalId, searchId, shownDocIds);
		}
	}
}
