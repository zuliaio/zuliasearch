package io.zulia.signals.storage;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.YearMonth;

class SignalsIndexConfigTest {

	@Test
	void singleIndexByDefault() {
		SignalsIndexConfig config = SignalsIndexConfig.defaults();
		Assertions.assertFalse(config.isPartitioned());
		Assertions.assertEquals("signals", config.readName());
		Assertions.assertEquals(config.readName(), config.writeName());
		Assertions.assertEquals("signals", config.clientIndexConfig().getIndexName());
		Assertions.assertThrows(IllegalStateException.class, () -> config.partitionName(YearMonth.of(2026, 8)));
	}

	@Test
	void partitionsAreOptIn() {
		SignalsIndexConfig config = SignalsIndexConfig.defaults().indexName("usage").monthlyPartitions();
		Assertions.assertEquals("usage-2026-08", config.partitionName(YearMonth.of(2026, 8)));
		Assertions.assertEquals(YearMonth.of(2026, 8), config.partitionMonth("usage-2026-08").orElseThrow());
		Assertions.assertTrue(config.partitionMonth("usage").isEmpty());
		Assertions.assertTrue(config.partitionMonth("usage-write").isEmpty());
		Assertions.assertEquals("usage", config.readName(), "reads go through the alias");
		Assertions.assertEquals(1, config.clientIndexConfig("usage-2026-08").getNumberOfShards(), "one shard");
	}

	@Test
	void zoneDefaultsToUtcAndIsRequired() {
		Assertions.assertEquals(java.time.ZoneOffset.UTC, SignalsIndexConfig.defaults().zone());
		Assertions.assertEquals(java.time.ZoneId.of("America/New_York"), SignalsIndexConfig.defaults().zone(java.time.ZoneId.of("America/New_York")).zone());
		Assertions.assertThrows(IllegalArgumentException.class, () -> SignalsIndexConfig.defaults().zone(null));
	}

	@Test
	void indexedTagsBecomeTagFieldsAndBadKeysAreRejected() {
		SignalsIndexConfig config = SignalsIndexConfig.defaults().indexTags("division", "projectType").indexTag("records", SignalField.Kind.LONG);
		Assertions.assertEquals(java.util.List.of("division", "projectType", "records"), java.util.List.copyOf(config.indexedTags().keySet()));
		Assertions.assertEquals(SignalField.Kind.KEYWORD_FACET, config.indexedTags().get("division"));
		@SuppressWarnings("removal") SignalsIndexConfig legacy = SignalsIndexConfig.defaults().dimensions("division", "projectType").dimension("records", SignalField.Kind.LONG);
		Assertions.assertEquals(config.indexedTags(), legacy.indexedTags(), "the deprecated names still index");
		Assertions.assertTrue(config.fieldNames().containsAll(java.util.List.of("tags.division", "tags.projectType", "tags.records")));
		Assertions.assertNotNull(config.clientIndexConfig().getFieldConfig("tags.division"));
		Assertions.assertNotNull(config.clientIndexConfig().getFieldConfig("tags.records"));
		Assertions.assertNull(config.clientIndexConfig().getFieldConfig("division"), "nothing at the top level");
		Assertions.assertThrows(IllegalArgumentException.class, () -> SignalsIndexConfig.defaults().indexTags(" "));
		Assertions.assertThrows(IllegalArgumentException.class, () -> SignalsIndexConfig.defaults().indexTags("division", "division"), "duplicate");
		Assertions.assertThrows(IllegalArgumentException.class, () -> SignalsIndexConfig.defaults().indexTags("division", null));
		Assertions.assertThrows(IllegalArgumentException.class, () -> SignalsIndexConfig.defaults().indexTags("a.b"), "dotted key");
		Assertions.assertThrows(IllegalArgumentException.class, () -> SignalsIndexConfig.defaults().indexTag("x", SignalField.Kind.STORED_ONLY),
				"stored only");
		Assertions.assertThrows(IllegalArgumentException.class, () -> SignalsIndexConfig.defaults().indexTag("x", null), "null kind");
	}

	@Test
	void schemaCoversEveryTypedField() {
		Assertions.assertTrue(SignalsIndexConfig.defaults().fieldNames().containsAll(
				java.util.List.of(SignalField.DAY.fieldName(), SignalField.ACTOR_ID.fieldName(), SignalField.SEARCH_QUERY.fieldName(),
						SignalField.SEARCH_LATENCY_MS.fieldName())));
		Assertions.assertFalse(SignalsIndexConfig.defaults().fieldNames().contains(SignalField.SEARCH_SHOWN_DOC_IDS.fieldName()), "shown ids are stored only");
		Assertions.assertTrue(SignalsIndexConfig.defaults().fieldNames().containsAll(
				java.util.List.of(SignalField.SEARCH_QUERY_NORMALIZED.fieldName(), SignalField.SEARCH_SIGNAL_ID.fieldName(),
						SignalField.SEARCH_ID.fieldName())));
		for (SignalField field : SignalField.values()) {
			Assertions.assertEquals(field.indexed(), SignalsIndexConfig.defaults().clientIndexConfig().getFieldConfig(field.fieldName()) != null, field.name());
		}
		Assertions.assertEquals(SignalField.ACTOR_ID, SignalField.byName("actorId").orElseThrow());
		Assertions.assertTrue(SignalField.byName("nope").isEmpty());
	}
}
