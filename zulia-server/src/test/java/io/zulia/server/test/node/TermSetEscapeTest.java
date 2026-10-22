package io.zulia.server.test.node;

import io.zulia.DefaultAnalyzers;
import io.zulia.client.command.Store;
import io.zulia.client.command.builder.FilterQuery;
import io.zulia.client.command.builder.Search;
import io.zulia.client.command.factory.Terms;
import io.zulia.client.command.factory.Values;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.client.pool.ZuliaWorkPool;
import io.zulia.doc.ResultDocBuilder;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.server.test.node.shared.NodeExtension;
import org.bson.Document;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.List;

/**
 * Escape handling inside zl:tq values matches the rest of the query syntax, and the client helpers produce text that round-trips.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class TermSetEscapeTest {

	@RegisterExtension
	static final NodeExtension nodeExtension = new NodeExtension(1);

	private static final String INDEX = "termSetEscapeTest";

	private static final String FIELD = "identifierExact";

	private static final String ID_A = "10.1000/jrnl.2011-0102-a";
	private static final String ID_B = "10.1000/jrnl.2011-0345-b";
	private static final String ID_C = "10.1000/jrnl.2012-0017-c";
	private static final String WINDOWS_PATH = "share\\reports\\q1";
	private static final String QUOTED_VALUE = "12\" monitor";

	@Test
	@Order(1)
	public void createIndex() throws Exception {
		ZuliaWorkPool zuliaWorkPool = nodeExtension.getClient();

		ClientIndexConfig indexConfig = new ClientIndexConfig();
		indexConfig.addDefaultSearchField(FIELD);
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("id").indexAs(DefaultAnalyzers.KEYWORD));
		indexConfig.addFieldConfig(FieldConfigBuilder.createString("identifier").indexAs(DefaultAnalyzers.KEYWORD, FIELD));
		indexConfig.setIndexName(INDEX);
		indexConfig.setNumberOfShards(1);

		zuliaWorkPool.createIndex(indexConfig);
	}

	@Test
	@Order(2)
	public void index() throws Exception {
		store("1", ID_A);
		store("2", ID_B);
		store("3", ID_C);
		store("4", WINDOWS_PATH);
		store("5", QUOTED_VALUE);
	}

	private void store(String id, String identifier) throws Exception {
		Document document = new Document();
		document.put("id", id);
		document.put("identifier", identifier);
		nodeExtension.getClient().store(new Store(id, INDEX).setResultDocument(ResultDocBuilder.newBuilder().setDocument(document)));
	}

	@Test
	@Order(3)
	public void slashForms() throws Exception {
		// a plain term escapes the slash
		Assertions.assertEquals(1, hits(FIELD + ":10.1000\\/jrnl.2011-0102-a"));
		Assertions.assertEquals(2, hits(FIELD + ":10.1000\\/jrnl.2011-0102-a OR " + FIELD + ":10.1000\\/jrnl.2011-0345-b"));

		// inside a term set every spelling of the slash matches the same documents
		Assertions.assertEquals(2, hits(FIELD + ":zl:tq(\"10.1000/jrnl.2011-0102-a\" \"10.1000/jrnl.2011-0345-b\")"));
		Assertions.assertEquals(2, hits(FIELD + ":zl:tq(\"10.1000\\/jrnl.2011-0102-a\" \"10.1000\\/jrnl.2011-0345-b\")"));
		Assertions.assertEquals(2, hits(FIELD + ":zl:tq(10.1000\\/jrnl.2011-0102-a 10.1000\\/jrnl.2011-0345-b)"));
		Assertions.assertEquals(2, hits("zl:tq(\"10.1000/jrnl.2011-0102-a\" 10.1000\\/jrnl.2011-0345-b)"));

		// an unquoted bare slash is still a syntax error because it starts a regex
		Assertions.assertThrows(Exception.class, () -> hits(FIELD + ":zl:tq(10.1000/jrnl.2011-0102-a)"));
	}

	@Test
	@Order(4)
	public void backslashAndQuoteForms() throws Exception {
		// a literal backslash is doubled, a single backslash escapes the character after it
		Assertions.assertEquals(1, hits(FIELD + ":zl:tq(\"share\\\\reports\\\\q1\")"));
		Assertions.assertEquals(0, hits(FIELD + ":zl:tq(\"share\\reports\\q1\")"));
		Assertions.assertEquals(1, hits(FIELD + ":\"share\\\\reports\\\\q1\""));

		// an escaped quote inside a quoted value is part of the term
		Assertions.assertEquals(1, hits(FIELD + ":zl:tq(\"12\\\" monitor\")"));
		Assertions.assertEquals(1, hits(FIELD + ":\"12\\\" monitor\""));
	}

	@Test
	@Order(5)
	public void clientRoundTrip() throws Exception {
		// the client quotes and escapes so any Java string reaches the index unchanged
		Assertions.assertEquals(2, hits(Terms.withField(FIELD).of(ID_A, ID_B).asString()));
		Assertions.assertEquals(3, hits(Terms.withField(FIELD).of(List.of(ID_A, ID_B, ID_C)).asString()));
		Assertions.assertEquals(1, hits(Terms.withField(FIELD).of(WINDOWS_PATH).asString()));
		Assertions.assertEquals(1, hits(Terms.withField(FIELD).of(QUOTED_VALUE).asString()));
		Assertions.assertEquals(2, hits(Terms.withField(FIELD).of(ID_A, WINDOWS_PATH).asString()));

		Assertions.assertEquals(1, hits(Values.single(ID_A).withFields(FIELD).asString()));
		Assertions.assertEquals(1, hits(Values.single(WINDOWS_PATH).withFields(FIELD).asString()));
		Assertions.assertEquals(1, hits(Values.single(QUOTED_VALUE).withFields(FIELD).asString()));
		Assertions.assertEquals(2, hits(Values.any().of(ID_B, WINDOWS_PATH).withFields(FIELD).asString()));
	}

	private long hits(String query) throws Exception {
		Search search = new Search(INDEX).setRealtime(true);
		search.addQuery(new FilterQuery(query));
		return nodeExtension.getClient().search(search).getTotalHits();
	}

}
