package io.zulia.server.test.util;

import com.google.protobuf.ByteString;
import io.zulia.util.ResultHelper;
import io.zulia.util.ZuliaUtil;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ResultHelperTest {

	@Test
	public void testFieldExtraction() throws Exception {

		Document testMongoDocument = new Document();
		testMongoDocument.put("field1", "someVal");
		testMongoDocument.put("myfield", 40);

		Document embeddedDocumentOne = new Document();
		embeddedDocumentOne.put("subfield1", "val2");

		Document embeddedDocumentTwo = new Document();
		embeddedDocumentTwo.put("otherfield", "1");
		embeddedDocumentOne.put("subfield2", embeddedDocumentTwo);

		testMongoDocument.put("field2", embeddedDocumentOne);

		List<Document> docs = new ArrayList<>();
		Document embeddedDocumentThree = new Document();
		embeddedDocumentThree.put("key1", "val1");
		embeddedDocumentThree.put("key2", "val2");
		Document embeddedDocumentFour = new Document();
		embeddedDocumentFour.put("key1", "someval");
		docs.add(embeddedDocumentThree);
		docs.add(embeddedDocumentFour);
		testMongoDocument.put("thisfield", docs);

		Assertions.assertEquals(Arrays.asList("val1", "someval"), ResultHelper.getValueFromMongoDocument(testMongoDocument, "thisfield.key1"));
		Assertions.assertEquals(Arrays.asList("val2"), ResultHelper.getValueFromMongoDocument(testMongoDocument, "thisfield.key2"));

		Assertions.assertEquals("1", ResultHelper.getValueFromMongoDocument(testMongoDocument, "field2.subfield2.otherfield"));
		Assertions.assertEquals(null, ResultHelper.getValueFromMongoDocument(testMongoDocument, "field2.subfield2.otherfield1"));
		Assertions.assertEquals(null, ResultHelper.getValueFromMongoDocument(testMongoDocument, "field2.subfield1.otherfield"));
		Assertions.assertEquals(null, ResultHelper.getValueFromMongoDocument(testMongoDocument, "thing"));
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
}
