package io.zulia.server.index;

import io.zulia.message.ZuliaIndex.FieldConfig;
import io.zulia.message.ZuliaIndex.FieldConfig.FieldType;
import io.zulia.message.ZuliaIndex.IndexSettings;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 *  Test that any new field defaults on unless it opts out, whether the index is new or the field is added to an existing one, while a field that already exists stays frozen to
 *  its persisted flag. (Lucene treats the skip index as immutable field schema)
 */
public class DocValueSkipIndexPolicyTest {

	private static FieldConfig field(String name) {
		return FieldConfig.newBuilder().setStoredFieldName(name).setFieldType(FieldType.NUMERIC_INT).build();
	}

	private static FieldConfig field(String name, boolean skip) {
		return FieldConfig.newBuilder().setStoredFieldName(name).setFieldType(FieldType.NUMERIC_INT).setDocValueSkipIndex(skip).build();
	}

	private static IndexSettings settings(FieldConfig... fieldConfigs) {
		IndexSettings.Builder builder = IndexSettings.newBuilder().setIndexName("x");
		for (FieldConfig fieldConfig : fieldConfigs) {
			builder.addFieldConfig(fieldConfig);
		}
		return builder.build();
	}

	private static boolean skipOf(IndexSettings resolved, String name) {
		return resolved.getFieldConfigList().stream().filter(fc -> fc.getStoredFieldName().equals(name)).findFirst().orElseThrow().getDocValueSkipIndex();
	}

	@Test
	public void newIndexDefaultsOnUnlessOptedOut() {
		IndexSettings resolved = ZuliaIndexManager.applyDocValueSkipIndexPolicy(settings(field("a"), field("b", false)), null);

		Assertions.assertTrue(skipOf(resolved, "a"), "unset field on a new index should default on");
		Assertions.assertFalse(skipOf(resolved, "b"), "explicit opt-out should be preserved");
	}

	@Test
	public void newFieldOnExistingIndexDefaultsOn() {
		IndexSettings existing = settings(field("a", false));
		IndexSettings resolved = ZuliaIndexManager.applyDocValueSkipIndexPolicy(settings(field("a"), field("b")), existing);

		Assertions.assertFalse(skipOf(resolved, "a"), "existing field must stay frozen to its persisted value");
		Assertions.assertTrue(skipOf(resolved, "b"), "field newly added to an existing index should default on");
	}

	@Test
	public void existingFieldFlagIsFrozenAgainstTheRequest() {
		IndexSettings existing = settings(field("a", true));
		// An older client re-sends settings without the flag, which reads as false; the on-disk schema must still win.
		IndexSettings resolved = ZuliaIndexManager.applyDocValueSkipIndexPolicy(settings(field("a", false)), existing);

		Assertions.assertTrue(skipOf(resolved, "a"), "existing field's persisted schema must win over the request");
	}
}
