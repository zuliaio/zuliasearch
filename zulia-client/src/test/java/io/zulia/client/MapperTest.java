package io.zulia.client;

import io.zulia.fields.Mapper;
import org.bson.Document;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Set;

public class MapperTest {

	@Test
	public void testSimpleCase() throws Exception {

		Date d = new Date();
		TestObj1 testObj1 = new TestObj1("test", 14, Arrays.asList("1", "2"), Set.of(4, 6, 7), d);

		Mapper<TestObj1> mapper = new Mapper<>(TestObj1.class);

		Document doc = mapper.toDocument(testObj1);

		Assert.assertEquals(doc.getString("field1"), "test");
		Assert.assertEquals((int) doc.getInteger("field2"), 14);
		Assert.assertEquals(doc.getList("field3", String.class).size(), 2);
		Assert.assertEquals(((Collection<Integer>) doc.get("field4")).size(), 3);
		Assert.assertEquals(d, doc.getDate("field5"));

		TestObj1 testObj1a = mapper.fromDocument(doc);
		Assert.assertEquals(testObj1.getField1(), testObj1a.getField1());
		Assert.assertEquals(testObj1.getField2(), testObj1a.getField2());
		Assert.assertEquals(testObj1.getField3(), testObj1a.getField3());
		Assert.assertEquals(testObj1.getField4(), testObj1a.getField4());
		Assert.assertEquals(testObj1.getField5(), testObj1a.getField5());

		System.out.println(doc);

	}

}
