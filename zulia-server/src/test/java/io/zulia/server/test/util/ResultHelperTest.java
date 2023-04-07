package io.zulia.server.test.util;

import com.google.protobuf.ByteString;
import io.zulia.util.ResultHelper;
import io.zulia.util.ZuliaUtil;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class ResultHelperTest {

	@Test
	public void testFieldExtraction() {

		Document testMongoDocument = new Document();
		testMongoDocument.put("field1", "someVal");
		testMongoDocument.put("myfield", 40);
		testMongoDocument.put("listField", List.of(1, 2, 3));

		Document embeddedDocumentOne = new Document();
		embeddedDocumentOne.put("subfield1", "val2");

		Document embeddedDocumentTwo = new Document();
		embeddedDocumentTwo.put("otherfield", "1");
		embeddedDocumentTwo.put("otherfield2", List.of("1", "2", "3"));
		embeddedDocumentOne.put("subfield2", embeddedDocumentTwo);

		testMongoDocument.put("field2", embeddedDocumentOne);

		List<Document> docs = new ArrayList<>();
		Document embeddedDocumentThree = new Document();
		embeddedDocumentThree.put("key1", "val1");
		embeddedDocumentThree.put("key2", "val2");
		embeddedDocumentThree.put("listKey", List.of("one", "two"));
		Document embeddedDocumentFour = new Document();
		embeddedDocumentFour.put("key1", "someval");
		embeddedDocumentFour.put("listKey", List.of("three"));
		embeddedDocumentFour.put("subDoc", new Document("subListKey", List.of(3)).append("subKeyA", "some text B"));
		Document embeddedDocumentFive = new Document();
		embeddedDocumentFive.put("subDoc", new Document("subListKey", List.of(1, 2)).append("subKeyA", "some text"));
		docs.add(embeddedDocumentThree);
		docs.add(embeddedDocumentFour);
		docs.add(embeddedDocumentFive);
		testMongoDocument.put("thisfield", docs);

		Assertions.assertEquals(List.of("val1", "someval"), ResultHelper.getValueFromMongoDocument(testMongoDocument, "thisfield.key1"));
		Assertions.assertEquals(List.of("val2"), ResultHelper.getValueFromMongoDocument(testMongoDocument, "thisfield.key2"));
		Assertions.assertEquals(List.of(List.of("one", "two"), List.of("three")),
				ResultHelper.getValueFromMongoDocument(testMongoDocument, "thisfield.listKey"));
		Assertions.assertEquals(List.of("some text B", "some text"), ResultHelper.getValueFromMongoDocument(testMongoDocument, "thisfield.subDoc.subKeyA"));
		Assertions.assertEquals(List.of(List.of(3), List.of(1, 2)), ResultHelper.getValueFromMongoDocument(testMongoDocument, "thisfield.subDoc.subListKey"));
		Assertions.assertEquals(List.of(1, 2, 3), ResultHelper.getValueFromMongoDocument(testMongoDocument, "listField"));

		Assertions.assertEquals("1", ResultHelper.getValueFromMongoDocument(testMongoDocument, "field2.subfield2.otherfield"));
		Assertions.assertEquals(List.of("1", "2", "3"), ResultHelper.getValueFromMongoDocument(testMongoDocument, "field2.subfield2.otherfield2"));
		Assertions.assertNull(ResultHelper.getValueFromMongoDocument(testMongoDocument, "field2.subfield2.otherfield1"));
		Assertions.assertNull(ResultHelper.getValueFromMongoDocument(testMongoDocument, "field2.subfield1.otherfield"));
		Assertions.assertNull(ResultHelper.getValueFromMongoDocument(testMongoDocument, "thing"));
		Assertions.assertEquals("someVal", ResultHelper.getValueFromMongoDocument(testMongoDocument, "field1"));
		Assertions.assertEquals("val2", ResultHelper.getValueFromMongoDocument(testMongoDocument, "field2.subfield1"));
		Assertions.assertEquals(40, ResultHelper.getValueFromMongoDocument(testMongoDocument, "myfield"));

	}

	@Test
	public void testSerialization() {
		Document testMongoDocument = new Document();
		testMongoDocument.put("field1", "someVal");
		testMongoDocument.put("myfield", 40);

		{
			byte[] bytes = ZuliaUtil.mongoDocumentToByteArray(testMongoDocument);
			Document testMongoDocumentReborn = ZuliaUtil.byteArrayToMongoDocument(bytes);

			Assertions.assertEquals("someVal", testMongoDocumentReborn.getString("field1"));
			Assertions.assertEquals(40, (int) testMongoDocumentReborn.getInteger("myfield"));
		}

		{
			ByteString byteString = ZuliaUtil.mongoDocumentToByteString(testMongoDocument);
			Document testMongoDocumentReborn = ZuliaUtil.byteStringToMongoDocument(byteString);

			Assertions.assertEquals("someVal", testMongoDocumentReborn.getString("field1"));
			Assertions.assertEquals(40, (int) testMongoDocumentReborn.getInteger("myfield"));
		}
	}

	@Test
	public void testFromJson() {
		String json = """
				{
				  "id": 1,
				  "title": "iPhone 11",
				  "category": [
				    "Mobile Phone",
				    "Electronics"
				  ],
				  "rating": 4.3,
				  "description": "iPhone 11, 64 GB"
				}""";

		Document document = ZuliaUtil.jsonToMongoDocument(json);

		Assertions.assertEquals(1, document.getInteger("id"));
		Assertions.assertEquals("iPhone 11", document.getString("title"));
		Assertions.assertEquals(List.of("Mobile Phone", "Electronics"), document.getList("category", String.class));
		Assertions.assertEquals(4.3, document.getDouble("rating"), 0.0001);
		Assertions.assertEquals("iPhone 11, 64 GB", document.getString("description"));

		String jsonOut = ZuliaUtil.mongoDocumentToJson(document, true);
		Assertions.assertEquals(json, jsonOut);
	}

}
