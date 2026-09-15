package io.zulia.server.test.util;

import io.zulia.util.document.DocumentHelper;
import io.zulia.util.document.DocumentHelper.ListElements;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

/**
 * The list element modes of the dotted path lookup. DROP_NULL is the plain lookup. RETAIN_NULL keeps a placeholder for
 * every element that is null or lacks the sub-field, at any depth. RETAIN_NULL_NESTED does the same but keeps a nested
 * list as one entry instead of flattening it. Empty strings are values in every mode. Every case asserts the first two
 * modes, and the deprecated boolean overload, so they are always compared side by side. The nested mode has its own cases.
 */
public class DocumentHelperNullsTest {

	private static final List<Object> NULL_NULL = Arrays.asList(null, null);

	@Test
	public void topLevelValuesAreReturnedAsStoredInEveryMode() {
		Document book = new Document("title", "Dune").append("subtitle", "").append("edition", null).append("tags", Arrays.asList("scifi", null, ""));

		assertBothModes(book, "title", "Dune", "Dune");
		assertBothModes(book, "subtitle", "", "");
		assertBothModes(book, "edition", null, null);
		assertBothModes(book, "missing", null, null);
		// a top-level list is returned as stored, nulls are only filtered while stepping through a dot
		assertBothModes(book, "tags", Arrays.asList("scifi", null, ""), Arrays.asList("scifi", null, ""));
	}

	@Test
	public void listInsideASubDocument() {
		Document book = new Document("meta", new Document("tags", Arrays.asList("scifi", null, "")));

		assertBothModes(book, "meta.tags", List.of("scifi", ""), Arrays.asList("scifi", null, ""));
	}

	@Test
	public void subFieldPresentAbsentEmptyAndNullAcrossAListOfDocuments() {
		Document book = new Document("authors",
				List.of(new Document("name", "A").append("affiliation", "MIT"), new Document("name", "B"), new Document("name", "C").append("affiliation", ""),
						new Document("name", "D").append("affiliation", null)));

		// an empty string is a value in both modes, only nulls and missing sub-fields are treated differently
		assertBothModes(book, "authors.affiliation", List.of("MIT", ""), Arrays.asList("MIT", null, "", null));
		assertBothModes(book, "authors.name", List.of("A", "B", "C", "D"), List.of("A", "B", "C", "D"));
	}

	@Test
	public void everyElementLackingTheSubFieldIsPlaceholdersNotNothing() {
		Document book = new Document("authors", List.of(new Document("name", "A"), new Document("name", "B")));

		assertBothModes(book, "authors.affiliation", null, NULL_NULL);
	}

	@Test
	public void nullAndScalarElementsInAListOfDocuments() {
		Document book = new Document("authors", Arrays.asList(new Document("name", "A"), null, "not a document", new Document("name", "B")));

		// a null element and a scalar element both hold a place in the retaining modes, so the result lines up with authors
		assertBothModes(book, "authors.name", List.of("A", "B"), Arrays.asList("A", null, null, "B"));
	}

	@Test
	public void emptyLists() {
		Document book = new Document("authors", List.of()).append("chapters", List.of(new Document("number", 1).append("sections", List.of())));

		assertBothModes(book, "authors.name", null, null);
		// a chapter whose section list is empty contributes nothing, not a placeholder
		assertBothModes(book, "chapters.sections.heading", null, null);
		assertBothModes(book, "chapters.number", List.of(1), List.of(1));
	}

	@Test
	public void descendsThroughAListOfLists() {
		Document book = new Document("chapters",
				List.of(new Document("number", 1).append("sections", List.of(new Document("heading", "Arrival"), new Document("heading", "The Desert"))),
						new Document("number", 2),
						new Document("number", 3).append("sections", List.of(new Document("heading", "Return"), new Document("pages", 12)))));

		// one placeholder for the chapter without sections, one for the section without a heading
		assertBothModes(book, "chapters.sections.heading", List.of("Arrival", "The Desert", "Return"),
				Arrays.asList("Arrival", "The Desert", null, "Return", null));
		// the intermediate step itself is one entry per chapter
		Assertions.assertEquals(3, ((List<?>) lookup(book, "chapters.sections", ListElements.RETAIN_NULL)).size());
		Assertions.assertEquals(2, ((List<?>) lookup(book, "chapters.sections", ListElements.DROP_NULL)).size());
	}

	@Test
	public void descendsThreeLevelsAndFlattensEachLevel() {
		Document book = new Document("chapters", List.of(new Document("sections",
						List.of(new Document("paragraphs", List.of(new Document("words", 10), new Document("words", 20))), new Document("paragraphs", List.of()),
								new Document("title", "no paragraphs"))),
				new Document("sections", List.of(new Document("paragraphs", List.of(new Document("words", 30)))))));

		assertBothModes(book, "chapters.sections.paragraphs.words", List.of(10, 20, 30), Arrays.asList(10, 20, null, 30));
	}

	@Test
	public void placeholderFromAnIntermediateStepCarriesThrough() {
		Document book = new Document("chapters",
				List.of(new Document("summary", new Document("words", 100)), new Document("summary", null), new Document("number", 2),
						new Document("summary", new Document("words", 200))));

		assertBothModes(book, "chapters.summary.words", List.of(100, 200), Arrays.asList(100, null, null, 200));
	}

	@Test
	public void nestedModeKeepsOneEntryPerOuterElement() {
		// chapter 2 has no section list, chapter 3 has a section without a heading, chapter 4 has an empty section list
		Document book = new Document("chapters",
				List.of(new Document("number", 1).append("sections", List.of(new Document("heading", "Arrival"), new Document("heading", "The Desert"))),
						new Document("number", 2),
						new Document("number", 3).append("sections", List.of(new Document("heading", "Return"), new Document("pages", 12))),
						new Document("number", 4).append("sections", List.of())));

		Assertions.assertEquals(Arrays.asList(List.of("Arrival", "The Desert"), null, Arrays.asList("Return", null), List.of()),
				lookup(book, "chapters.sections.heading", ListElements.RETAIN_NULL_NESTED));
		// which lines up with the sibling path one level up, unlike the flat modes
		Assertions.assertEquals(List.of(1, 2, 3, 4), lookup(book, "chapters.number", ListElements.RETAIN_NULL_NESTED));
		Assertions.assertEquals(Arrays.asList("Arrival", "The Desert", null, "Return", null),
				lookup(book, "chapters.sections.heading", ListElements.RETAIN_NULL));
		Assertions.assertEquals(List.of("Arrival", "The Desert", "Return"), lookup(book, "chapters.sections.heading", ListElements.DROP_NULL));
	}

	@Test
	public void nestedModeAgreesWithRetainNullWhenNothingIsNested() {
		Document book = new Document("authors", List.of(new Document("name", "A").append("affiliation", "MIT"), new Document("name", "B"))).append("meta",
				new Document("tags", Arrays.asList("scifi", null, "")));

		Assertions.assertEquals(Arrays.asList("MIT", null), lookup(book, "authors.affiliation", ListElements.RETAIN_NULL_NESTED));
		Assertions.assertEquals(Arrays.asList("scifi", null, ""), lookup(book, "meta.tags", ListElements.RETAIN_NULL_NESTED));
		// two missing levels still yield one placeholder per author, in both retaining modes
		Assertions.assertEquals(NULL_NULL, lookup(book, "authors.missing.deeper", ListElements.RETAIN_NULL));
		Assertions.assertEquals(NULL_NULL, lookup(book, "authors.missing.deeper", ListElements.RETAIN_NULL_NESTED));
	}

	@Test
	public void nestedModeKeepsEveryLevel() {
		Document book = new Document("chapters", List.of(new Document("sections",
						List.of(new Document("paragraphs", List.of(new Document("words", 10), new Document("words", 20))), new Document("paragraphs", List.of()),
								new Document("title", "no paragraphs"))),
				new Document("sections", List.of(new Document("paragraphs", List.of(new Document("words", 30)))))));

		Assertions.assertEquals(List.of(Arrays.asList(List.of(10, 20), List.of(), null), List.of(List.of(30))),
				lookup(book, "chapters.sections.paragraphs.words", ListElements.RETAIN_NULL_NESTED));
		Assertions.assertEquals(Arrays.asList(10, 20, null, 30), lookup(book, "chapters.sections.paragraphs.words", ListElements.RETAIN_NULL));
	}

	@Test
	public void aLeafThatIsAListStaysNested() {
		Document book = new Document("chapters", List.of(new Document("keywords", List.of("sand", "spice")), new Document("keywords", List.of("water"))));

		List<Object> nested = List.of(List.of("sand", "spice"), List.of("water"));
		assertBothModes(book, "chapters.keywords", nested, nested);
	}

	@Test
	public void pathsThatCannotBeFollowed() {
		Document book = new Document("title", "Dune").append("pages", 412).append("meta", null);

		// a scalar or a null cannot be stepped into, and a document without the first key resolves to nothing
		assertBothModes(book, "pages.count", null, null);
		assertBothModes(book, "title.first", null, null);
		assertBothModes(book, "meta.tags", null, null);
		assertBothModes(book, "missing.deeper", null, null);
	}

	@SuppressWarnings("deprecation")
	private static void assertBothModes(Document doc, String path, Object plain, Object retainNull) {
		Assertions.assertEquals(plain, lookup(doc, path, ListElements.DROP_NULL), path + " DROP_NULL");
		Assertions.assertEquals(retainNull, lookup(doc, path, ListElements.RETAIN_NULL), path + " RETAIN_NULL");
		// the two-argument lookup is the plain mode and the deprecated boolean overload maps onto the two modes
		Assertions.assertEquals(plain, DocumentHelper.getValueFromMongoDocument(doc, path), path + " two-argument lookup");
		Assertions.assertEquals(plain, DocumentHelper.getValueFromMongoDocument(doc, path, false), path + " boolean false");
		Assertions.assertEquals(retainNull, DocumentHelper.getValueFromMongoDocument(doc, path, true), path + " boolean true");
	}

	private static Object lookup(Document doc, String path, ListElements listElements) {
		return DocumentHelper.getValueFromMongoDocument(doc, path, listElements);
	}
}
