package io.zulia.signals.storage;

import io.zulia.signals.model.Signal;
import io.zulia.signals.model.SearchDetails;
import io.zulia.signals.model.Target;
import org.bson.Document;

import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.temporal.IsoFields;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

public final class SignalEnricher {

	private static final Pattern WHITESPACE = Pattern.compile("\\s+");

	private final Clock clock;
	private final ZoneId bucketZone;
	private final ActorIdMapper actorIdMapper;
	private final Map<String, SignalField.Kind> dimensions;

	public SignalEnricher(SignalsIndexConfig config, Clock clock) {
		this(config, clock, ActorIdMapper.identity());
	}

	public SignalEnricher(SignalsIndexConfig config, Clock clock, ActorIdMapper actorIdMapper) {
		this(clock, config.zone(), actorIdMapper, config.dimensions());
	}

	public SignalEnricher(Clock clock, ZoneId bucketZone, ActorIdMapper actorIdMapper) {
		this(clock, bucketZone, actorIdMapper, Map.of());
	}

	public SignalEnricher(Clock clock, ZoneId bucketZone, ActorIdMapper actorIdMapper, Map<String, SignalField.Kind> dimensions) {
		this.clock = clock;
		this.bucketZone = bucketZone;
		this.actorIdMapper = actorIdMapper;
		this.dimensions = Map.copyOf(dimensions);
	}

	/** The actor id as it is stored, so reports can filter on a real id under a mapping {@link ActorIdMapper}. */
	public String storedActorId(String app, String actorId) {
		return actorIdMapper.map(app, actorId);
	}

	public Document toDocument(Signal signal) {
		LocalDate eventDate = signal.timestamp().atZone(bucketZone).toLocalDate();
		Map<String, Object> fields = new LinkedHashMap<>();
		put(fields, SignalField.SIGNAL_ID, signal.signalId());
		put(fields, SignalField.TIMESTAMP, Date.from(signal.timestamp()));
		put(fields, SignalField.RECEIVED_AT, Date.from(Instant.now(clock)));
		put(fields, SignalField.DAY, day(eventDate));
		put(fields, SignalField.WEEK, week(eventDate));
		put(fields, SignalField.MONTH, month(eventDate));
		put(fields, SignalField.APP, signal.app());
		putIfPresent(fields, SignalField.CLIENT, signal.client());
		putIfPresent(fields, SignalField.SESSION_ID, signal.sessionId());
		put(fields, SignalField.ACTOR_ID, actorIdMapper.map(signal.app(), signal.actor().id()));
		put(fields, SignalField.ACTOR_TYPE, signal.actor().type().name());
		if (signal.actor().delegatedBy() != null) {
			put(fields, SignalField.DELEGATED_BY, actorIdMapper.map(signal.app(), signal.actor().delegatedBy()));
		}
		put(fields, SignalField.ACTION_TYPE, signal.actionType());
		Target target = signal.target();
		if (target != null) {
			put(fields, SignalField.TARGET_TYPE, target.type());
			// one id stays the scalar it was given, a bulk stores the list so the facet counts every id
			put(fields, SignalField.TARGET_ID, target.ids().size() == 1 ? target.ids().getFirst() : target.ids());
		}
		SearchDetails search = signal.search();
		if (search != null) {
			putIfPresent(fields, SignalField.SEARCH_QUERY, search.query());
			if (search.query() != null) {
				put(fields, SignalField.SEARCH_QUERY_NORMALIZED, normalizeQuery(search.query()));
			}
			if (!search.indexes().isEmpty()) {
				put(fields, SignalField.SEARCH_INDEX, search.indexes());
			}
			putIfPresent(fields, SignalField.SEARCH_RESULT_COUNT, search.resultCount());
			putIfPresent(fields, SignalField.SEARCH_LATENCY_MS, search.latencyMs());
			putIfPresent(fields, SignalField.SEARCH_CLICKED_POSITION, search.clickedPosition());
			putIfPresent(fields, SignalField.SEARCH_CLICKED_DOC_ID, search.clickedDocId());
			putIfPresent(fields, SignalField.SEARCH_SIGNAL_ID, search.searchSignalId());
			putIfPresent(fields, SignalField.SEARCH_ID, search.searchId());
			if (!search.shownDocIds().isEmpty()) {
				put(fields, SignalField.SEARCH_SHOWN_DOC_IDS, search.shownDocIds());
			}
		}
		putIfPresent(fields, SignalField.DURATION_MS, signal.durationMs());
		if (!signal.tags().isEmpty()) {
			fields.put(SignalField.TAGS, tagsDocument(signal));
		}
		return new Document(fields);
	}

	// a declared dimension is stored in its declared kind so the index config and the document never disagree
	private Document tagsDocument(Signal signal) {
		Document tags = new Document();
		for (Map.Entry<String, Object> tag : signal.tags().entrySet()) {
			SignalField.Kind kind = dimensions.get(tag.getKey());
			tags.put(tag.getKey(), kind == null ? tag.getValue() : asKind(signal, tag.getKey(), tag.getValue(), kind));
		}
		return tags;
	}

	private static Object asKind(Signal signal, String key, Object value, SignalField.Kind kind) {
		return switch (kind) {
			case KEYWORD, KEYWORD_FACET, TEXT -> switch (value) {
				case Date date -> date.toInstant().toString();
				case Collection<?> values -> values.stream().map(String::valueOf).toList();
				default -> String.valueOf(value);
			};
			case LONG -> value instanceof Number number ? number.longValue() : mismatch(signal, key, value, kind);
			case INT -> value instanceof Number number ? intValue(signal, key, number) : mismatch(signal, key, value, kind);
			case DATE -> value instanceof Date ? value : mismatch(signal, key, value, kind);
			case STORED_ONLY -> value;
		};
	}

	private static Object intValue(Signal signal, String key, Number number) {
		try {
			return Math.toIntExact(number.longValue());
		}
		catch (ArithmeticException e) {
			throw new IllegalArgumentException(
					"Tag " + key + " is declared as INT but signal " + signal.signalId() + " carries " + number + ", which does not fit an int", e);
		}
	}

	private static Object mismatch(Signal signal, String key, Object value, SignalField.Kind kind) {
		throw new IllegalArgumentException(
				"Tag " + key + " is declared as " + kind + " but signal " + signal.signalId() + " carries " + value.getClass().getSimpleName() + " " + value);
	}

	public static String normalizeQuery(String query) {
		return WHITESPACE.matcher(query.trim().toLowerCase(Locale.ROOT)).replaceAll(" ");
	}

	public static String day(LocalDate date) {
		return date.toString();
	}

	public static String week(LocalDate date) {
		return String.format(Locale.ROOT, "%d-W%02d", date.get(IsoFields.WEEK_BASED_YEAR), date.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR));
	}

	public static String month(LocalDate date) {
		return YearMonth.from(date).toString();
	}

	private static void put(Map<String, Object> fields, SignalField field, Object value) {
		fields.put(field.fieldName(), value);
	}

	private static void putIfPresent(Map<String, Object> fields, SignalField field, Object value) {
		putIfPresent(fields, field.fieldName(), value);
	}

	private static void putIfPresent(Map<String, Object> fields, String key, Object value) {
		if (value != null) {
			fields.put(key, value);
		}
	}
}
