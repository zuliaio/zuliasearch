package io.zulia.testing.js;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;
import io.zulia.testing.TemplateResolver;
import io.zulia.testing.config.FunctionConfig;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.PolyglotException;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.proxy.ProxyExecutable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * One GraalVM JavaScript context for a test run. Binds globals, runs the script, defines functions, evaluates expressions, and converts results back
 * to Java. Every failure names the test, value, function or script that caused it.
 */
public final class JsContext implements AutoCloseable {
	private static final Logger LOG = LoggerFactory.getLogger(JsContext.class);

	/**
	 * Global that holds the saved values of a previous run, {@code saved.name}.
	 */
	public static final String SAVED = "saved";

	/**
	 * Global function for date math, {@code date("now-1y/y")} or {@code date("oneYearAgo", "yyyy")}.
	 */
	public static final String DATE_FUNCTION = "date";

	private static final Pattern JS_IDENTIFIER = Pattern.compile("[A-Za-z_$][\\w$]*");

	private static final Set<String> JS_RESERVED_WORDS = Set.of("await", "break", "case", "catch", "class", "const", "continue", "debugger", "default",
			"delete", "do", "else", "enum", "export", "extends", "false", "finally", "for", "function", "if", "implements", "import", "in", "instanceof",
			"interface", "let", "new", "null", "package", "private", "protected", "public", "return", "static", "super", "switch", "this", "throw", "true",
			"try", "typeof", "undefined", "var", "void", "while", "with", "yield");

	private static final Gson GSON = new GsonBuilder().setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE).create();

	private final Context context;
	private final Value globals;
	private final Value isUndefined;

	public JsContext() {
		context = Context.newBuilder("js").option("engine.WarnInterpreterOnly", "false").allowHostAccess(HostAccess.ALL).build();
		globals = context.getBindings("js");
		isUndefined = context.eval("js", "v => v === undefined");
	}

	/**
	 * A name that can be bound as a global or used as a parameter: a valid identifier that is not a reserved word.
	 */
	public static void requireBindableName(String kind, String name) {
		if (name == null || !JS_IDENTIFIER.matcher(name).matches()) {
			throw new IllegalArgumentException(kind + " <" + name + "> is not a valid JavaScript identifier");
		}
		if (JS_RESERVED_WORDS.contains(name)) {
			throw new IllegalArgumentException(kind + " <" + name + "> is a reserved JavaScript word");
		}
	}

	/**
	 * Validate a function name, arguments, and that it has exactly one of expr or body.
	 */
	public static void validateFunction(FunctionConfig function) {
		String name = function.getName();
		requireBindableName("Function name", name);
		if (function.getArgs() != null) {
			for (String arg : function.getArgs()) {
				requireBindableName("Function <" + name + "> argument", arg);
			}
		}
		boolean hasExpr = function.getExpr() != null;
		boolean hasBody = function.getBody() != null;
		if (hasExpr == hasBody) {
			throw new IllegalArgumentException(
					"Function <" + name + "> must define exactly one of expr (a single expression) or body (statements with return)");
		}
	}

	/**
	 * Binds a global checking it does not shadow anything already defined like a search or variable cannot silently replace JSON, Math or an earlier binding.
	 */
	public void bindNew(String kind, String name, Object value) {
		requireUnused(kind, name);
		globals.putMember(name, value);
	}

	/**
	 * True when a global with this name exists, whether a binding or a JavaScript built-in.
	 */
	public boolean isDefined(String name) {
		return globals.hasMember(name);
	}

	private void requireUnused(String kind, String name) {
		if (isDefined(name)) {
			throw new IllegalArgumentException(kind + " name <" + name + "> is already taken by a search result, a variable, " + SAVED + ", " + DATE_FUNCTION
					+ ", an earlier function or a JavaScript built-in");
		}
	}

	/**
	 * Binds {@code date(expression)} and {@code date(expression, pattern)} so scripts get date math without {@code ${}} inside JavaScript.
	 */
	public void bindDateFunction(TemplateResolver templates) {
		ProxyExecutable dateFunction = arguments -> {
			if (arguments.length < 1 || arguments.length > 2 || !arguments[0].isString() || (arguments.length == 2 && !arguments[1].isString())) {
				throw new IllegalArgumentException(DATE_FUNCTION + "() takes a date expression and an optional format pattern, both strings");
			}
			String placeholder = arguments.length == 2 ? arguments[0].asString() + "|" + arguments[1].asString() : arguments[0].asString();
			return templates.resolve("${" + placeholder + "}");
		};
		bindNew("Function", DATE_FUNCTION, dateFunction);
	}

	/**
	 * JSON text of a global, for logging.
	 */
	public String toJson(String globalName) {
		return context.eval("js", "JSON.stringify(" + globalName + ")").asString();
	}

	/**
	 * Runs statements in the global scope, so anything they declare is visible after running.
	 */
	public void runScript(String name, String script) {
		try {
			context.eval(Source.newBuilder("js", script, name).buildLiteral());
		}
		catch (PolyglotException e) {
			throw new IllegalArgumentException("Evaluating script <" + name + "> failed: " + failureReason(e), e);
		}
	}

	/**
	 * Declares a config function in the global scope. A single {@code expr} is templated like a test expression. A {@code body} is a JavaScript
	 * block like the script, so it is left alone and uses {@code date()} and the variable globals instead of {@code ${}}.
	 */
	public void defineFunction(FunctionConfig function, TemplateResolver templates) {
		validateFunction(function);
		String name = function.getName();
		requireUnused("Function", name);
		List<String> args = function.getArgs() != null ? function.getArgs() : List.of();
		boolean hasExpr = function.getExpr() != null;
		// The closing parenthesis goes on its own line so an expr that ends in a // comment still compiles
		String body = hasExpr ? "return (" + templates.resolve(function.getExpr()) + "\n);" : function.getBody();
		String source = "function " + name + "(" + String.join(", ", args) + ") {\n" + body + "\n}";
		LOG.info("Defining function {}({})", name, String.join(", ", args));
		evaluate("function", name, source);
	}

	/**
	 * Evaluates one expression. {@code kind} and {@code name} identify it in the failure message. A read of a saved value that no earlier run
	 * recorded surfaces as {@link SavedUnavailableException} so the caller can skip the expression.
	 */
	public Value evaluate(String kind, String name, String expr) {
		try {
			return context.eval("js", expr);
		}
		catch (PolyglotException e) {
			if (e.isHostException() && e.asHostException() instanceof SavedUnavailableException savedUnavailable) {
				throw savedUnavailable;
			}
			throw new IllegalArgumentException("Evaluating " + kind + " <" + name + "> expression <" + expr + "> failed: " + failureReason(e), e);
		}
	}

	/**
	 * Converts a recorded value to something Gson can write. Primitives convert directly, anything structured goes through JSON so documents,
	 * facets and arrays come out as plain maps and lists. Undefined is rejected rather than recorded as null, since it almost always means a typo.
	 */
	public Object toJava(String name, Value value) {
		if (isUndefined.execute(value).asBoolean()) {
			throw new IllegalArgumentException("Value <" + name + "> evaluated to undefined. Check the expression for a misspelled property or search name");
		}
		if (value.isNull()) {
			return null;
		}
		if (value.isBoolean()) {
			return value.asBoolean();
		}
		if (value.isNumber()) {
			// Separate returns on purpose, a ternary would widen the long to a double
			if (value.fitsInLong()) {
				return value.asLong();
			}
			if (!value.fitsInDouble()) {
				throw new IllegalArgumentException(
						"Value <" + name + "> is <" + value + ">, a number too large to record exactly. Convert it with Number() or String()");
			}
			double number = value.asDouble();
			if (!Double.isFinite(number)) {
				throw new IllegalArgumentException("Value <" + name + "> is " + number + ", which cannot be written to the run report");
			}
			return number;
		}
		if (value.isString()) {
			return value.asString();
		}
		Value json = context.eval("js", "JSON.stringify").execute(value);
		if (isUndefined.execute(json).asBoolean()) {
			throw new IllegalArgumentException("Value <" + name + "> is a function or symbol and cannot be recorded");
		}
		return GSON.fromJson(json.asString(), Object.class);
	}

	private static String failureReason(PolyglotException e) {
		return e.isHostException() ? e.asHostException().getMessage() : e.getMessage();
	}

	@Override
	public void close() {
		context.close();
	}
}
