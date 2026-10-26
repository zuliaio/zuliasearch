package io.zulia.testing.config;

import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeSet;

/**
 * Every config class field must appear in the published YAML schema and vice versa, so an editor validating a test file
 * never rejects a key the runner accepts.
 */
public class SchemaFieldCoverageTest {

	private static final String SCHEMA = "/zulia_testing_schema-1.1.json";

	private static final Map<String, Class<?>> DEFINITIONS = Map.of("ZuliaTesting", ZuliaTestConfig.class, "Connection", ConnectionConfig.class, "Index",
			IndexConfig.class, "Search", SearchConfig.class, "Query", QueryConfig.class, "Facet", FacetConfig.class, "StatFacet", StatFacetConfig.class,
			"NumStat", NumStatConfig.class, "Test", TestConfig.class);

	@Test
	public void everyConfigFieldIsInTheSchema() throws Exception {
		Document definitions = loadSchema().get("definitions", Document.class);
		Assertions.assertEquals(new TreeSet<>(DEFINITIONS.keySet()), new TreeSet<>(definitions.keySet()), "schema definitions and config classes differ");
		for (Map.Entry<String, Class<?>> entry : DEFINITIONS.entrySet()) {
			Document definition = definitions.get(entry.getKey(), Document.class);
			TreeSet<String> schemaFields = new TreeSet<>(definition.get("properties", Document.class).keySet());
			TreeSet<String> classFields = new TreeSet<>(
					Arrays.stream(entry.getValue().getDeclaredFields()).filter(SchemaFieldCoverageTest::isConfigField).map(Field::getName).toList());
			Assertions.assertEquals(classFields, schemaFields, entry.getValue().getSimpleName() + " fields differ from schema definition " + entry.getKey());
		}
	}

	private static boolean isConfigField(Field field) {
		int modifiers = field.getModifiers();
		return !Modifier.isStatic(modifiers) && !field.isSynthetic();
	}

	private static Document loadSchema() throws Exception {
		try (InputStream in = SchemaFieldCoverageTest.class.getResourceAsStream(SCHEMA)) {
			Assertions.assertNotNull(in, "schema resource " + SCHEMA + " not found");
			return Document.parse(new String(in.readAllBytes(), StandardCharsets.UTF_8));
		}
	}
}
