package io.zulia.data.test;

import io.zulia.data.target.spreadsheet.delimited.formatter.NumberCSVWriter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

public class NumberCSVWriterTest {

	@Test
	void standardNumberTypesAreFormatted() {
		NumberCSVWriter<List<String>> writer = new NumberCSVWriter<>();
		List<String> out = new ArrayList<>();

		writer.writeType(out, 42);
		writer.writeType(out, 42L);
		writer.writeType(out, 1.5f);
		writer.writeType(out, 2.5d);

		Assertions.assertEquals(List.of("42", "42", "1.500", "2.500"), out);
	}

	@Test
	void otherNumberSubtypesKeepTheirValueInsteadOfWritingNull() {
		// Integer/Long/Float/Double were the only matched cases; every other Number subtype fell through to a
		// null cell, silently dropping data (notably Decimal128 from Mongo decimal fields). They must now write.
		NumberCSVWriter<List<String>> writer = new NumberCSVWriter<>();
		List<String> out = new ArrayList<>();

		writer.writeType(out, new BigInteger("123456789012345678901234567890"));
		writer.writeType(out, new BigDecimal("1.25"));
		writer.writeType(out, (short) 7);
		writer.writeType(out, (byte) 3);

		Assertions.assertEquals(List.of("123456789012345678901234567890", "1.25", "7", "3"), out);
	}

	@Test
	void nullStillWritesAnEmptyCell() {
		NumberCSVWriter<List<String>> writer = new NumberCSVWriter<>();
		List<String> out = new ArrayList<>();

		writer.writeType(out, null);

		Assertions.assertEquals(1, out.size());
		Assertions.assertNull(out.getFirst());
	}
}
