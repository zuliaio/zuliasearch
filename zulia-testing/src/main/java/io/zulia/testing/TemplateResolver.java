package io.zulia.testing;

import java.time.DateTimeException;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAdjusters;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Substitutes {@code ${...}} placeholders in query strings and expressions.
 * A placeholder is a configured variable name, date math anchored on {@code now} ({@code now-1y}, {@code now-90d}, {@code now-1y/y}), or a date
 * alias such as {@code today} or {@code startOfLastYear}. An optional {@code |pattern} suffix formats a date, the default is yyyy-MM-dd.
 */
public final class TemplateResolver {

	private static final Pattern PLACEHOLDER = Pattern.compile("\\$\\{([^}]*)}");
	private static final Pattern DATE_MATH = Pattern.compile("now((?:[+-]\\d+[dwMy])*)(?:/([dwMy]))?");
	private static final Pattern DATE_OFFSET = Pattern.compile("([+-])(\\d+)([dwMy])");

	private static final Map<String, String> ALIASES = new TreeMap<>(
			Map.ofEntries(Map.entry("today", "now"), Map.entry("yesterday", "now-1d"), Map.entry("oneWeekAgo", "now-1w"), Map.entry("oneMonthAgo", "now-1M"),
					Map.entry("oneYearAgo", "now-1y"), Map.entry("startOfWeek", "now/w"), Map.entry("startOfMonth", "now/M"), Map.entry("startOfYear", "now/y"),
					Map.entry("startOfLastMonth", "now-1M/M"), Map.entry("startOfLastYear", "now-1y/y")));

	private final LocalDate now;
	private final Map<String, Object> variables;

	public TemplateResolver(LocalDate now) {
		this(now, Map.of());
	}

	public TemplateResolver(LocalDate now, Map<String, Object> variables) {
		this.now = now;
		this.variables = variables;
	}

	/**
	 * True for names the resolver owns, which a variable may not reuse.
	 */
	public static boolean isReservedDateName(String name) {
		return "now".equals(name) || ALIASES.containsKey(name);
	}

	public LocalDate now() {
		return now;
	}

	public static Map<String, String> aliases() {
		return Collections.unmodifiableMap(ALIASES);
	}

	/**
	 * Replaces every placeholder in the template. Text without placeholders is returned as is.
	 */
	public String resolve(String template) {
		if (template == null || !template.contains("${")) {
			return template;
		}
		Matcher matcher = PLACEHOLDER.matcher(template);
		StringBuilder sb = new StringBuilder();
		while (matcher.find()) {
			int nested = matcher.group(1).indexOf("${");
			if (nested >= 0) {
				// The closing brace found belongs to a later placeholder, so this one was never closed
				String unclosed = template.substring(matcher.start(), matcher.start(1) + nested).trim();
				throw new IllegalArgumentException("Unterminated ${ placeholder <" + unclosed + "> in template <" + template + ">. Add the closing }");
			}
			matcher.appendReplacement(sb, Matcher.quoteReplacement(resolvePlaceholder(matcher.group(1).trim(), template)));
		}
		matcher.appendTail(sb);
		if (sb.indexOf("${") >= 0) {
			throw new IllegalArgumentException("Unterminated ${ placeholder in template <" + template + ">");
		}
		return sb.toString();
	}

	private String resolvePlaceholder(String placeholder, String template) {
		int pipe = placeholder.indexOf('|');
		String expression = pipe < 0 ? placeholder : placeholder.substring(0, pipe).trim();
		String pattern = pipe < 0 ? null : placeholder.substring(pipe + 1).trim();

		if (variables.containsKey(expression)) {
			if (pattern != null) {
				throw new IllegalArgumentException(
						"Variable <" + expression + "> in template <" + template + "> cannot take a format, formats apply to dates only");
			}
			return String.valueOf(variables.get(expression));
		}

		LocalDate date = evaluate(expression, template);
		if (pattern == null || pattern.isEmpty()) {
			return date.toString();
		}
		try {
			return DateTimeFormatter.ofPattern(pattern).format(date);
		}
		catch (IllegalArgumentException | DateTimeException e) {
			throw new IllegalArgumentException("Invalid date format <" + pattern + "> in template <" + template + ">: " + e.getMessage(), e);
		}
	}

	/**
	 * Evaluates one expression without a format, such as {@code now-1y/y} or {@code oneYearAgo}.
	 */
	public LocalDate evaluate(String expression) {
		return evaluate(expression, expression);
	}

	private LocalDate evaluate(String expression, String template) {
		String dateMath = ALIASES.getOrDefault(expression, expression);
		Matcher matcher = DATE_MATH.matcher(dateMath);
		if (!matcher.matches()) {
			throw new IllegalArgumentException(
					"Unknown template variable <" + expression + "> in <" + template + ">. Use a configured variable " + variables.keySet()
							+ ", date math anchored on now (now-1y, now-90d, now-1y/y) or one of " + ALIASES.keySet());
		}

		try {
			return applyDateMath(matcher.group(1), matcher.group(2));
		}
		catch (DateTimeException | ArithmeticException | NumberFormatException e) {
			throw new IllegalArgumentException("Date math <" + expression + "> in <" + template + "> is out of range: " + e.getMessage(), e);
		}
	}

	private LocalDate applyDateMath(String offsetText, String rounding) {
		LocalDate date = now;
		Matcher offsets = DATE_OFFSET.matcher(offsetText);
		while (offsets.find()) {
			long amount = Long.parseLong(offsets.group(2));
			if (offsets.group(1).equals("-")) {
				amount = -amount;
			}
			date = switch (offsets.group(3)) {
				case "d" -> date.plusDays(amount);
				case "w" -> date.plusWeeks(amount);
				case "M" -> date.plusMonths(amount);
				case "y" -> date.plusYears(amount);
				default -> throw new IllegalStateException("Unreachable unit <" + offsets.group(3) + ">");
			};
		}

		if (rounding != null) {
			date = switch (rounding) {
				case "d" -> date;
				case "w" -> date.with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY));
				case "M" -> date.withDayOfMonth(1);
				case "y" -> date.withDayOfYear(1);
				default -> throw new IllegalStateException("Unreachable rounding <" + rounding + ">");
			};
		}
		return date;
	}
}
