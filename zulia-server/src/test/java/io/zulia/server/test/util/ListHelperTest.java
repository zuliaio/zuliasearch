package io.zulia.server.test.util;

import io.zulia.util.ZuliaUtil;
import io.zulia.util.document.DocumentHelper;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class ListHelperTest {

	@Test
	public void handleLists() {

		List<String> items = List.of("Item One", "Item Two", "Item Three");
		List<Object> results = new ArrayList<>();

		ZuliaUtil.handleLists(items, results::add);

		Assertions.assertEquals(items.size(), results.size());

	}

	@Test
	public void handleListsUniqueValues() {

		List<String> items = List.of("Item One", "Item Two", "Item Three", "Item Three", "Item Four", "Item Four");
		List<Object> results = new ArrayList<>();

		ZuliaUtil.handleListsUniqueValues(items, results::add);

		Assertions.assertNotEquals(items.size(), results.size());
		Assertions.assertEquals(4, results.size());

	}

	@Test
	public void handleListsRetainNullAndEmpty() {

		Document doc = new Document();
		doc.put("subDocs", new ArrayList<>());

		for (int i = 0; i < 3; i++) {
			Document subDoc = new Document();
			subDoc.put("name", "John Doe " + i);
			if (i != 1) {
				subDoc.put("id", "1234" + i);
			}
			else {
				subDoc.put("id", null);
			}

			doc.getList("subDocs", Document.class).add(subDoc);
		}

		Object o = DocumentHelper.getValueFromMongoDocument(doc, "subDocs.name", true);
		List<Object> results = new ArrayList<>();
		ZuliaUtil.handleListsRetainNullAndEmpty(o, results::add);
		Assertions.assertEquals(3, results.size());

		o = DocumentHelper.getValueFromMongoDocument(doc, "subDocs.id", true);
		results = new ArrayList<>();
		ZuliaUtil.handleListsRetainNullAndEmpty(o, results::add);
		Assertions.assertEquals(3, results.size());

		results = new ArrayList<>();
		ZuliaUtil.handleLists(o, results::add);

		Assertions.assertEquals(2, results.size());

	}
}
