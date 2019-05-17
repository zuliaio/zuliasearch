package io.zulia.client;

import io.zulia.fields.Mapper;
import org.bson.Document;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Set;

public class MapperTest {

	@Test
	public void testSimpleCase() throws Exception {

		TestObj1 testObj1 = new TestObj1("test", 14, Arrays.asList("1", "2"), Set.of(4, 6, 7));

		Mapper<TestObj1> mapper = new Mapper<>(TestObj1.class);

		Document doc = mapper.toDocument(testObj1);

		Assert.assertEquals(doc.getString("field1"), "test");
		Assert.assertEquals((int) doc.getInteger("field2"), 14);
		Assert.assertEquals(doc.getList("field3", String.class).size(), 2);
		Assert.assertEquals(((Set<Integer>) doc.get("field4")).size(), 3);

		System.out.println(doc);

	}

}
