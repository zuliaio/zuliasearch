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
	private final Map<String, SignalField.Kind> dimensions;

	private SignalsIndexConfig(String indexName, boolean monthlyPartitions, ZoneId zone, Map<String, SignalField.Kind> dimensions) {
		this.indexName = indexName;
		this.monthlyPartitions = monthlyPartitions;
		this.zone = zone;
		this.dimensions = dimensions;
	}

	public static SignalsIndexConfig defaults() {
		return new SignalsIndexConfig(DEFAULT_INDEX_NAME, false, ZoneOffset.UTC, Map.of());
	}

	public SignalsIndexConfig indexName(String indexName) {
		if (indexName == null || indexName.isBlank()) {
			throw new IllegalArgumentException("Index name is required but was " + (indexName == null ? "null" : "blank"));
		}
		return new SignalsIndexConfig(indexName, monthlyPartitions, zone, dimensions);
	}

	/** Retention drops indexes instead of paging deletes. */
	public SignalsIndexConfig monthlyPartitions() {
		return new SignalsIndexConfig(indexName, true, zone, dimensions);
	}

	/** Zone for buckets, partition months, and retention cutoffs. UTC by default. */
	public SignalsIndexConfig zone(ZoneId zone) {
		if (zone == null) {
			throw new IllegalArgumentException("Zone is required, pass ZoneOffset.UTC for the default");
		}
		return new SignalsIndexConfig(indexName, monthlyPartitions, zone, dimensions);
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

	/** Tag keys indexed as keyword facets, the common case. Typed dimensions go through {@link #dimension(String, SignalField.Kind)}. */
	public SignalsIndexConfig dimensions(String... keys) {
		if (keys == null) {
			throw new IllegalArgumentException("Dimensions require the tag keys to index but got a null array");
		}
		SignalsIndexConfig config = this;
		for (String key : keys) {
			config = config.dimension(key, SignalField.Kind.KEYWORD_FACET);
		}
		return config;
	}

	/** One tag key indexed as {@code tags.<key>} with the given kind. Undeclared tags are stored, not indexed. */
	public SignalsIndexConfig dimension(String key, SignalField.Kind kind) {
		if (key == null || key.isBlank()) {
			throw new IllegalArgumentException("Dimension key must not be null or blank, declared so far: " + dimensions.keySet());
		}
		if (key.indexOf('.') >= 0 || key.indexOf('$') >= 0) {
			throw new IllegalArgumentException("Dimension key " + key + " must not contain '.' or '$', it becomes a field of the tags document");
		}
		if (kind == null || kind == SignalField.Kind.STORED_ONLY) {
			throw new IllegalArgumentException("Dimension " + key + " needs an indexed kind, got " + kind + ". Tags are stored without being declared");
		}
		if (dimensions.containsKey(key)) {
			throw new IllegalArgumentException("Dimension " + key + " is already declared as " + dimensions.get(key));
		}
		Map<String, SignalField.Kind> declared = new LinkedHashMap<>(dimensions);
		declared.put(key, kind);
		return new SignalsIndexConfig(indexName, monthlyPartitions, zone, Collections.unmodifiableMap(declared));
	}

	public Map<String, SignalField.Kind> dimensions() {
		return dimensions;
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
				dimensions.keySet().stream().map(SignalField::tagField)).toList();
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
		for (Map.Entry<String, SignalField.Kind> dimension : dimensions.entrySet()) {
			config.addFieldConfig(dimension.getValue().fieldConfig(SignalField.tagField(dimension.getKey())).orElseThrow());
		}
		return config;
	}

}
