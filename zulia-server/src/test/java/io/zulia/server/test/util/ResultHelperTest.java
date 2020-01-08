package io.zulia.server.test.util;

import com.google.protobuf.ByteString;
import io.zulia.util.ResultHelper;
import io.zulia.util.ZuliaUtil;
import org.bson.Document;
import org.testng.Assert;
import org.testng.annotations.Test;

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

		Assert.assertEquals(ResultHelper.getValueFromMongoDocument(testMongoDocument, "thisfield.key1"), Arrays.asList("val1", "someval"));
		Assert.assertEquals(ResultHelper.getValueFromMongoDocument(testMongoDocument, "thisfield.key2"), Arrays.asList("val2"));

		Assert.assertEquals(ResultHelper.getValueFromMongoDocument(testMongoDocument, "field2.subfield2.otherfield"), "1");
		Assert.assertEquals(ResultHelper.getValueFromMongoDocument(testMongoDocument, "field2.subfield2.otherfield1"), null);
		Assert.assertEquals(ResultHelper.getValueFromMongoDocument(testMongoDocument, "field2.subfield1.otherfield"), null);
		Assert.assertEquals(ResultHelper.getValueFromMongoDocument(testMongoDocument, "thing"), null);
		Assert.assertEquals(ResultHelper.getValueFromMongoDocument(testMongoDocument, "field1"), "someVal");
		Assert.assertEquals(ResultHelper.getValueFromMongoDocument(testMongoDocument, "field2.subfield1"), "val2");
		Assert.assertEquals(ResultHelper.getValueFromMongoDocument(testMongoDocument, "myfield"), 40);

	}

	@Test
	public void testSerialization() {
		Document testMongoDocument = new Document();
		testMongoDocument.put("field1", "someVal");
		testMongoDocument.put("myfield", 40);

		{
			byte[] bytes = ZuliaUtil.mongoDocumentToByteArray(testMongoDocument);
			Document testMongoDocumentReborn = ZuliaUtil.byteArrayToMongoDocument(bytes);

			Assert.assertEquals(testMongoDocumentReborn.getString("field1"), "someVal");
			Assert.assertEquals((int) testMongoDocumentReborn.getInteger("myfield"), 40);
		}

		{
			ByteString byteString = ZuliaUtil.mongoDocumentToByteString(testMongoDocument);
			Document testMongoDocumentReborn = ZuliaUtil.byteStringToMongoDocument(byteString);

			Assert.assertEquals(testMongoDocumentReborn.getString("field1"), "someVal");
			Assert.assertEquals((int) testMongoDocumentReborn.getInteger("myfield"), 40);
		}
	}
}
