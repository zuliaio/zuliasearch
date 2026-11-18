package io.zulia.testing;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.time.LocalDate;

public class TemplateResolverTest {

	// 2026-10-22 is a Thursday
	private final TemplateResolver resolver = new TemplateResolver(LocalDate.of(2026, 10, 22));

	@ParameterizedTest
	@CsvSource({ "now, 2026-10-22", "now-1y, 2025-10-22", "now+1w, 2026-10-29", "now-90d, 2026-07-24", "now-3M, 2026-07-22", "now/y, 2026-01-01",
			"now/M, 2026-10-01", "now/w, 2026-10-19", "now/d, 2026-10-22", "now-1y/y, 2025-01-01", "now-1M/M, 2026-09-01", "now-1y+1d, 2025-10-23",
			"now-2y-6M, 2024-04-22" })
	public void dateMath(String expression, String expected) {
		Assertions.assertEquals(LocalDate.parse(expected), resolver.evaluate(expression));
	}

	@ParameterizedTest
	@CsvSource({ "today, 2026-10-22", "yesterday, 2026-10-21", "oneWeekAgo, 2026-10-15", "oneMonthAgo, 2026-09-22", "oneYearAgo, 2025-10-22",
			"startOfWeek, 2026-10-19", "startOfMonth, 2026-10-01", "startOfYear, 2026-01-01", "startOfLastMonth, 2026-09-01", "startOfLastYear, 2025-01-01" })
	public void aliases(String alias, String expected) {
		Assertions.assertEquals(LocalDate.parse(expected), resolver.evaluate(alias));
	}

	@Test
	public void substitutesInsideText() {
		Assertions.assertEquals("pubDate:2026-10-22", resolver.resolve("pubDate:${today}"));
		Assertions.assertEquals("pubDate:[2025-10-22 TO 2026-10-22]", resolver.resolve("pubDate:[${oneYearAgo} TO ${today}]"));
		Assertions.assertEquals("pubDate:[2025-01-01 TO 2025-10-22]", resolver.resolve("pubDate:[${now-1y/y} TO ${now-1y}]"));
		Assertions.assertEquals("pubDate:[2025-01-01 TO 2025-10-22]", resolver.resolve("pubDate:[${ startOfLastYear } TO ${ oneYearAgo }]"));
	}

	@Test
	public void formats() {
		Assertions.assertEquals("pubYear:2026", resolver.resolve("pubYear:${today|yyyy}"));
		Assertions.assertEquals("pubYear:2025", resolver.resolve("pubYear:${now-1y|yyyy}"));
		Assertions.assertEquals("10/22/2026", resolver.resolve("${today|MM/dd/yyyy}"));
		Assertions.assertEquals("2026-10", resolver.resolve("${today | yyyy-MM }"));
		Assertions.assertEquals("label == \"2025\"", resolver.resolve("label == \"${oneYearAgo|yyyy}\""));
	}

	@Test
	public void leavesPlainTextAlone() {
		Assertions.assertNull(resolver.resolve(null));
		Assertions.assertEquals("title:cancer", resolver.resolve("title:cancer"));
		Assertions.assertEquals("a $ b { c } $x", resolver.resolve("a $ b { c } $x"));
	}

	@Test
	public void luceneExclusiveRangeBracesAreNotPlaceholders() {
		Assertions.assertEquals("pubYear:{2000 TO 2010]", resolver.resolve("pubYear:{2000 TO 2010]"));
		Assertions.assertEquals("pubYear:[2000 TO 2010}", resolver.resolve("pubYear:[2000 TO 2010}"));
		Assertions.assertEquals("pubYear:{2025 TO 2026]", resolver.resolve("pubYear:{${oneYearAgo|yyyy} TO ${today|yyyy}]"));
		Assertions.assertEquals("pubDate:[2025-10-22 TO 2026-10-22}", resolver.resolve("pubDate:[${oneYearAgo} TO ${today}}"));
	}

	@Test
	public void monthArithmeticClampsToMonthEnd() {
		TemplateResolver endOfMarch = new TemplateResolver(LocalDate.of(2026, 3, 31));
		Assertions.assertEquals(LocalDate.of(2026, 2, 28), endOfMarch.evaluate("now-1M"));
		Assertions.assertEquals(LocalDate.of(2024, 2, 29), new TemplateResolver(LocalDate.of(2025, 2, 28)).evaluate("now-1y+1d"));
	}

	@Test
	public void unknownVariableIsDescriptive() {
		IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class, () -> resolver.resolve("pubDate:${tomorrow}"));
		Assertions.assertTrue(e.getMessage().contains("<tomorrow>"), e.getMessage());
		Assertions.assertTrue(e.getMessage().contains("pubDate:${tomorrow}"), e.getMessage());
		Assertions.assertTrue(e.getMessage().contains("startOfLastYear"), e.getMessage());

		Assertions.assertThrows(IllegalArgumentException.class, () -> resolver.evaluate("now-1x"));
		Assertions.assertThrows(IllegalArgumentException.class, () -> resolver.evaluate("now-1y/q"));
		Assertions.assertThrows(IllegalArgumentException.class, () -> resolver.evaluate("now 1y"));
		Assertions.assertThrows(IllegalArgumentException.class, () -> resolver.evaluate(""));
	}

	@Test
	public void unterminatedPlaceholderIsRejected() {
		IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class, () -> resolver.resolve("pubDate:${today"));
		Assertions.assertTrue(e.getMessage().contains("Unterminated"), e.getMessage());
		Assertions.assertTrue(e.getMessage().contains("pubDate:${today"), e.getMessage());
	}

	@Test
	public void unterminatedPlaceholderBeforeAClosedOneIsReportedAsUnterminated() {
		IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class, () -> resolver.resolve("pubDate:[${startOfYear TO ${today}]"));
		Assertions.assertTrue(e.getMessage().contains("Unterminated ${ placeholder <${startOfYear TO>"), e.getMessage());
		Assertions.assertFalse(e.getMessage().contains("Unknown template variable"), e.getMessage());
	}

	@Test
	public void dateMathOutOfRangeIsRejectedWithContext() {
		IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class, () -> resolver.resolve("${now-9999999999y}"));
		Assertions.assertTrue(e.getMessage().contains("Date math <now-9999999999y>"), e.getMessage());
		Assertions.assertTrue(e.getMessage().contains("out of range"), e.getMessage());

		Assertions.assertThrows(IllegalArgumentException.class, () -> resolver.resolve("${now+99999999999999999999d}"));
		Assertions.assertThrows(UnsupportedOperationException.class, () -> TemplateResolver.aliases().put("x", "now"));
	}

	@Test
	public void timePatternOnADateIsRejectedWithContext() {
		IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class, () -> resolver.resolve("${today|yyyy-MM-dd HH:mm}"));
		Assertions.assertTrue(e.getMessage().contains("Invalid date format <yyyy-MM-dd HH:mm>"), e.getMessage());
	}

	@Test
	public void badFormatIsDescriptive() {
		IllegalArgumentException e = Assertions.assertThrows(IllegalArgumentException.class, () -> resolver.resolve("${today|yyyy-b}"));
		Assertions.assertTrue(e.getMessage().contains("Invalid date format <yyyy-b>"), e.getMessage());
	}
}
