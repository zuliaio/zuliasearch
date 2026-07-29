package io.zulia.util.test;

import io.zulia.util.BooleanUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

public class BooleanUtilTest {

	@Test
	void trueFormsParseAsTrue() {
		for (String value : List.of("true", "TRUE", "True", "t", "T", "yes", "YES", "y", "Y", "1", " true ", "\ty\t")) {
			Assertions.assertEquals(Boolean.TRUE, BooleanUtil.parseBoolean(value), "expected <" + value + "> to parse as true");
		}
	}

	@Test
	void falseFormsParseAsFalse() {
		for (String value : List.of("false", "FALSE", "False", "f", "F", "no", "NO", "n", "N", "0", " false ", "\tn\t")) {
			Assertions.assertEquals(Boolean.FALSE, BooleanUtil.parseBoolean(value), "expected <" + value + "> to parse as false");
		}
	}

	@Test
	void unrecognizedStringsParseAsNull() {
		Assertions.assertNull(BooleanUtil.parseBoolean((String) null));
		Assertions.assertNull(BooleanUtil.parseBoolean(""));
		Assertions.assertNull(BooleanUtil.parseBoolean("   "));
		Assertions.assertNull(BooleanUtil.parseBoolean("maybe"));
		Assertions.assertNull(BooleanUtil.parseBoolean("2"));
		Assertions.assertNull(BooleanUtil.parseBoolean("-1"));
	}

	@Test
	void numbersParseAsZeroAndOne() {
		Assertions.assertEquals(Boolean.TRUE, BooleanUtil.parseBoolean(1));
		Assertions.assertEquals(Boolean.TRUE, BooleanUtil.parseBoolean(1L));
		Assertions.assertEquals(Boolean.TRUE, BooleanUtil.parseBoolean(1.0));
		Assertions.assertEquals(Boolean.TRUE, BooleanUtil.parseBoolean(new BigDecimal("1.00")));
		Assertions.assertEquals(Boolean.FALSE, BooleanUtil.parseBoolean(0));
		Assertions.assertEquals(Boolean.FALSE, BooleanUtil.parseBoolean(0.0));
		Assertions.assertNull(BooleanUtil.parseBoolean(2));
		Assertions.assertNull(BooleanUtil.parseBoolean(0.5));
		Assertions.assertNull(BooleanUtil.parseBoolean((Number) null));
	}

	@Test
	void objectsParseByType() {
		Assertions.assertEquals(Boolean.TRUE, BooleanUtil.parseBoolean((Object) Boolean.TRUE));
		Assertions.assertEquals(Boolean.FALSE, BooleanUtil.parseBoolean((Object) Boolean.FALSE));
		Assertions.assertEquals(Boolean.TRUE, BooleanUtil.parseBoolean((Object) "Yes"));
		Assertions.assertEquals(Boolean.FALSE, BooleanUtil.parseBoolean((Object) 0));
		Assertions.assertNull(BooleanUtil.parseBoolean((Object) null));
		Assertions.assertNull(BooleanUtil.parseBoolean(List.of("true")));
	}

	@Test
	void booleanIntKeepsThreeStateContract() {
		Assertions.assertEquals(1, BooleanUtil.getStringAsBooleanInt("YeS"));
		Assertions.assertEquals(0, BooleanUtil.getStringAsBooleanInt("N"));
		Assertions.assertEquals(-1, BooleanUtil.getStringAsBooleanInt("unknown"));
		Assertions.assertEquals(-1, BooleanUtil.getStringAsBooleanInt(null));
	}
}
