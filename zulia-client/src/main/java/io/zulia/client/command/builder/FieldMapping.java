package io.zulia.client.command.builder;

import io.zulia.message.ZuliaIndex;

import java.util.LinkedHashSet;
import java.util.Set;

public class FieldMapping implements FieldMappingBuilder {

	private final String alias;
	private final Set<String> fields;
	private boolean includeSelf;

	public FieldMapping(String alias) {
		this.alias = alias;
		this.fields = new LinkedHashSet<>();
	}

	public FieldMapping addMappedFields(String... fieldOrFieldPatterns) {
		for (String fieldOrFieldPattern : fieldOrFieldPatterns) {
			fields.add(fieldOrFieldPattern);
		}
		return this;
	}

	public FieldMapping includeSelf() {
		this.includeSelf = true;
		return this;
	}

	public FieldMapping excludeSelf() {
		this.includeSelf = false;
		return this;
	}

	@Override
	public ZuliaIndex.FieldMapping getFieldMapping() {
		return ZuliaIndex.FieldMapping.newBuilder().setAlias(alias).addAllFieldOrFieldPattern(fields).setIncludeSelf(includeSelf).build();
	}
}
