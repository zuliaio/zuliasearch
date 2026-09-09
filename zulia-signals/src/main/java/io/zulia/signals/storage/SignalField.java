package io.zulia.signals.storage;

import io.zulia.DefaultAnalyzers;
import io.zulia.fields.FieldConfigBuilder;

import java.util.Arrays;
import java.util.Optional;

/** App dimensions are added by {@link SignalsIndexConfig#dimensions(String...)}. */
public enum SignalField {
	SIGNAL_ID("signalId", Kind.KEYWORD),
	TIMESTAMP("timestamp", Kind.DATE),
	RECEIVED_AT("receivedAt", Kind.DATE),
	DAY("day", Kind.KEYWORD_FACET),
	WEEK("week", Kind.KEYWORD_FACET),
	MONTH("month", Kind.KEYWORD_FACET),
	APP("app", Kind.KEYWORD_FACET),
	CLIENT("client", Kind.KEYWORD_FACET),
	SESSION_ID("sessionId", Kind.KEYWORD),
	ACTOR_ID("actorId", Kind.KEYWORD_FACET),
	ACTOR_TYPE("actorType", Kind.KEYWORD_FACET),
	DELEGATED_BY("delegatedBy", Kind.KEYWORD),
	ACTION_TYPE("actionType", Kind.KEYWORD_FACET),
	TARGET_TYPE("targetType", Kind.KEYWORD_FACET),
	TARGET_ID("targetId", Kind.KEYWORD_FACET),
	SEARCH_QUERY("searchQuery", Kind.TEXT),
	SEARCH_QUERY_NORMALIZED("searchQueryNormalized", Kind.KEYWORD_FACET),
	SEARCH_INDEX("searchIndex", Kind.KEYWORD_FACET),
	SEARCH_RESULT_COUNT("searchResultCount", Kind.LONG),
	SEARCH_LATENCY_MS("searchLatencyMs", Kind.LONG),
	SEARCH_CLICKED_POSITION("searchClickedPosition", Kind.INT),
	SEARCH_CLICKED_DOC_ID("searchClickedDocId", Kind.KEYWORD),
	SEARCH_SIGNAL_ID("searchSignalId", Kind.KEYWORD),
	SEARCH_ID("searchId", Kind.KEYWORD),
	SEARCH_SHOWN_DOC_IDS("searchShownDocIds", Kind.STORED_ONLY),
	DURATION_MS("durationMs", Kind.LONG);

	/** The sub document holding tags. A declared dimension is indexed as {@code tags.<key>}. */
	public static final String TAGS = "tags";

	public enum Kind {
		KEYWORD,
		KEYWORD_FACET,
		TEXT,
		DATE,
		LONG,
		INT,
		STORED_ONLY;

		public Optional<FieldConfigBuilder> fieldConfig(String fieldName) {
			return Optional.ofNullable(switch (this) {
				case KEYWORD -> FieldConfigBuilder.createString(fieldName).indexAs(DefaultAnalyzers.KEYWORD);
				case KEYWORD_FACET -> FieldConfigBuilder.createString(fieldName).indexAs(DefaultAnalyzers.KEYWORD).facet();
				case TEXT -> FieldConfigBuilder.createString(fieldName).indexAs(DefaultAnalyzers.STANDARD);
				case DATE -> FieldConfigBuilder.createDate(fieldName).index().sort();
				case LONG -> FieldConfigBuilder.createLong(fieldName).index().sort();
				case INT -> FieldConfigBuilder.createInt(fieldName).index().sort();
				case STORED_ONLY -> null;
			});
		}
	}

	private final String fieldName;
	private final Kind kind;

	SignalField(String fieldName, Kind kind) {
		this.fieldName = fieldName;
		this.kind = kind;
	}

	public String fieldName() {
		return fieldName;
	}

	public Kind kind() {
		return kind;
	}

	public boolean indexed() {
		return kind != Kind.STORED_ONLY;
	}

	public Optional<FieldConfigBuilder> fieldConfig() {
		return kind.fieldConfig(fieldName);
	}

	public static String tagField(String key) {
		return TAGS + "." + key;
	}

	public static Optional<SignalField> byName(String fieldName) {
		return Arrays.stream(values()).filter(field -> field.fieldName.equals(fieldName)).findFirst();
	}
}
