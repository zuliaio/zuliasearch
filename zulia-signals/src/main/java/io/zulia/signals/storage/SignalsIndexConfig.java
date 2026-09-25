package io.zulia.signals.storage;

import io.zulia.client.config.ClientIndexConfig;

import java.time.YearMonth;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

/** One index by default, or monthly partitions behind an alias whose write index is the current month. */
public final class SignalsIndexConfig {

	public static final String DEFAULT_INDEX_NAME = "signals";

	private final String indexName;
	private final boolean monthlyPartitions;
	private final ZoneId zone;
	private final Map<String, SignalField.Kind> indexedTags;

	private SignalsIndexConfig(String indexName, boolean monthlyPartitions, ZoneId zone, Map<String, SignalField.Kind> indexedTags) {
		this.indexName = indexName;
		this.monthlyPartitions = monthlyPartitions;
		this.zone = zone;
		this.indexedTags = indexedTags;
	}

	public static SignalsIndexConfig defaults() {
		return new SignalsIndexConfig(DEFAULT_INDEX_NAME, false, ZoneOffset.UTC, Map.of());
	}

	public SignalsIndexConfig indexName(String indexName) {
		if (indexName == null || indexName.isBlank()) {
			throw new IllegalArgumentException("Index name is required but was " + (indexName == null ? "null" : "blank"));
		}
		return new SignalsIndexConfig(indexName, monthlyPartitions, zone, indexedTags);
	}

	/** Retention drops indexes instead of paging deletes. */
	public SignalsIndexConfig monthlyPartitions() {
		return new SignalsIndexConfig(indexName, true, zone, indexedTags);
	}

	/** Zone for buckets, partition months, and retention cutoffs. UTC by default. */
	public SignalsIndexConfig zone(ZoneId zone) {
		if (zone == null) {
			throw new IllegalArgumentException("Zone is required, pass ZoneOffset.UTC for the default");
		}
		return new SignalsIndexConfig(indexName, monthlyPartitions, zone, indexedTags);
	}

	public String indexName() {
		return indexName;
	}

	public boolean isPartitioned() {
		return monthlyPartitions;
	}

	public ZoneId zone() {
		return zone;
	}

	/** Tag keys to index as keyword facets so reports can slice, filter, and tally on them. Typed keys go through {@link #indexTag(String, SignalField.Kind)}. */
	public SignalsIndexConfig indexTags(String... keys) {
		if (keys == null) {
			throw new IllegalArgumentException("Index tags requires the tag keys to index but got a null array");
		}
		SignalsIndexConfig config = this;
		for (String key : keys) {
			config = config.indexTag(key, SignalField.Kind.KEYWORD_FACET);
		}
		return config;
	}

	/** One tag key indexed as {@code tags.<key>} with the given kind. Tags that are not indexed are stored only. */
	public SignalsIndexConfig indexTag(String key, SignalField.Kind kind) {
		if (key == null || key.isBlank()) {
			throw new IllegalArgumentException("Tag key must not be null or blank, indexed so far: " + indexedTags.keySet());
		}
		if (key.indexOf('.') >= 0 || key.indexOf('$') >= 0) {
			throw new IllegalArgumentException("Tag key " + key + " must not contain '.' or '$', it becomes a field of the tags document");
		}
		if (kind == null || kind == SignalField.Kind.STORED_ONLY) {
			throw new IllegalArgumentException("Tag " + key + " needs an indexed kind, got " + kind + ". Tags are stored without being indexed");
		}
		if (indexedTags.containsKey(key)) {
			throw new IllegalArgumentException("Tag " + key + " is already indexed as " + indexedTags.get(key));
		}
		Map<String, SignalField.Kind> indexed = new LinkedHashMap<>(indexedTags);
		indexed.put(key, kind);
		return new SignalsIndexConfig(indexName, monthlyPartitions, zone, Collections.unmodifiableMap(indexed));
	}

	/** @deprecated use {@link #indexTags(String...)} */
	@Deprecated(forRemoval = true)
	public SignalsIndexConfig dimensions(String... keys) {
		return indexTags(keys);
	}

	/** @deprecated use {@link #indexTag(String, SignalField.Kind)} */
	@Deprecated(forRemoval = true)
	public SignalsIndexConfig dimension(String key, SignalField.Kind kind) {
		return indexTag(key, kind);
	}

	/** Indexed tag keys and their kinds, in declaration order. */
	public Map<String, SignalField.Kind> indexedTags() {
		return indexedTags;
	}

	/** @deprecated use {@link #indexedTags()} */
	@Deprecated(forRemoval = true)
	public Map<String, SignalField.Kind> dimensions() {
		return indexedTags;
	}

	/** The alias when partitioned. */
	public String readName() {
		return indexName;
	}

	/** The alias routes to its write index when partitioned. */
	public String writeName() {
		return indexName;
	}

	public String partitionName(YearMonth month) {
		if (!monthlyPartitions) {
			throw new IllegalStateException("Partition names only exist with monthly partitions enabled, index " + indexName + " is a single index");
		}
		return indexName + "-" + month;
	}

	/** Empty when the name is not one of this config's partitions. */
	public Optional<YearMonth> partitionMonth(String physicalIndexName) {
		String prefix = indexName + "-";
		if (!monthlyPartitions || !physicalIndexName.startsWith(prefix)) {
			return Optional.empty();
		}
		try {
			return Optional.of(YearMonth.parse(physicalIndexName.substring(prefix.length())));
		}
		catch (DateTimeParseException e) {
			return Optional.empty();
		}
	}

	/** Stored only fields are not listed. */
	public List<String> fieldNames() {
		return Stream.concat(Arrays.stream(SignalField.values()).filter(SignalField::indexed).map(SignalField::fieldName),
				indexedTags.keySet().stream().map(SignalField::tagField)).toList();
	}

	public ClientIndexConfig clientIndexConfig() {
		return clientIndexConfig(indexName);
	}

	public ClientIndexConfig clientIndexConfig(String physicalIndexName) {
		ClientIndexConfig config = new ClientIndexConfig();
		config.setIndexName(physicalIndexName);
		// one shard so facet counts are exact
		config.setNumberOfShards(1);
		for (SignalField field : SignalField.values()) {
			field.fieldConfig().ifPresent(config::addFieldConfig);
		}
		for (Map.Entry<String, SignalField.Kind> tag : indexedTags.entrySet()) {
			config.addFieldConfig(tag.getValue().fieldConfig(SignalField.tagField(tag.getKey())).orElseThrow());
		}
		return config;
	}

}
