package io.zulia.testing;

import io.zulia.testing.config.ZuliaTestConfig;
import io.zulia.testing.js.JsContext;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Resolves the config's {@code variables} with any overrides applied, ready to bind as JavaScript globals and to substitute into query strings.
 */
public final class TestVariables {

	private TestVariables() {
	}

	/**
	 * Variables resolve in declaration order. A string value may use date templates and any variable declared above it, so a range can be built
	 * from a start date defined on the line before. A variable cannot refer to one declared below it.
	 */
	public static Map<String, Object> resolve(ZuliaTestConfig config, Map<String, Object> overrides, LocalDate asOf) {
		Map<String, Object> declared = new LinkedHashMap<>();
		if (config.getVariables() != null) {
			declared.putAll(config.getVariables());
		}
		declared.putAll(overrides);

		Map<String, Object> resolved = new LinkedHashMap<>();
		for (Map.Entry<String, Object> entry : declared.entrySet()) {
			String name = entry.getKey();
			JsContext.requireBindableName("Variable name", name);
			if (TemplateResolver.isReservedDateName(name) || JsContext.SAVED.equals(name) || JsContext.DATE_FUNCTION.equals(name)) {
				throw new IllegalArgumentException(
						"Variable name <" + name + "> is reserved for date templates, " + JsContext.SAVED + " or " + JsContext.DATE_FUNCTION);
			}
			Object value = switch (entry.getValue()) {
				case String text -> new TemplateResolver(asOf, resolved).resolve(text);
				case Date date -> isoText(date);
				case null, default -> entry.getValue();
			};
			resolved.put(name, value);
		}
		return resolved;
	}

	/**
	 * YAML reads an unquoted {@code 2024-06-01} as a Date at midnight UTC. Give it back as the text the author wrote, or a full instant if it had a time.
	 */
	private static String isoText(Date date) {
		ZonedDateTime utc = Instant.ofEpochMilli(date.getTime()).atZone(ZoneOffset.UTC);
		return utc.toLocalTime().toSecondOfDay() == 0 && utc.getNano() == 0 ? utc.toLocalDate().toString() : utc.toInstant().toString();
	}
}
