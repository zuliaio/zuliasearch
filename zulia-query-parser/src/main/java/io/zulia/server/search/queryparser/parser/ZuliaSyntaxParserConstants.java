/* Generated By:JavaCC: Do not edit this line. ZuliaSyntaxParserConstants.java */
package io.zulia.server.search.queryparser.parser;

/**
 * Token literal values and constants.
 * Generated by org.javacc.parser.OtherFilesGen#start()
 */
public interface ZuliaSyntaxParserConstants {

	/** End of File. */
	int EOF = 0;
	/** RegularExpression Id. */
	int _NUM_CHAR = 1;
	/** RegularExpression Id. */
	int _ESCAPED_CHAR = 2;
	/** RegularExpression Id. */
	int _TERM_START_CHAR = 3;
	/** RegularExpression Id. */
	int _TERM_CHAR = 4;
	/** RegularExpression Id. */
	int _WHITESPACE = 5;
	/** RegularExpression Id. */
	int _QUOTED_CHAR = 6;
	/** RegularExpression Id. */
	int AND = 8;
	/** RegularExpression Id. */
	int OR = 9;
	/** RegularExpression Id. */
	int NOT = 10;
	/** RegularExpression Id. */
	int FN_PREFIX = 11;
	/** RegularExpression Id. */
	int PLUS = 12;
	/** RegularExpression Id. */
	int MINUS = 13;
	/** RegularExpression Id. */
	int RPAREN = 14;
	/** RegularExpression Id. */
	int OP_COLON = 15;
	/** RegularExpression Id. */
	int OP_EQUAL = 16;
	/** RegularExpression Id. */
	int OP_LESSTHAN = 17;
	/** RegularExpression Id. */
	int OP_LESSTHANEQ = 18;
	/** RegularExpression Id. */
	int OP_MORETHAN = 19;
	/** RegularExpression Id. */
	int OP_MORETHANEQ = 20;
	/** RegularExpression Id. */
	int CARAT = 21;
	/** RegularExpression Id. */
	int TILDE = 22;
	/** RegularExpression Id. */
	int QUOTED = 23;
	/** RegularExpression Id. */
	int NUMBER = 24;
	/** RegularExpression Id. */
	int TERM = 25;
	/** RegularExpression Id. */
	int REGEXPTERM = 26;
	/** RegularExpression Id. */
	int RANGEIN_START = 27;
	/** RegularExpression Id. */
	int RANGEEX_START = 28;
	/** RegularExpression Id. */
	int LPAREN = 29;
	/** RegularExpression Id. */
	int ATLEAST = 30;
	/** RegularExpression Id. */
	int AFTER = 31;
	/** RegularExpression Id. */
	int BEFORE = 32;
	/** RegularExpression Id. */
	int CONTAINED_BY = 33;
	/** RegularExpression Id. */
	int CONTAINING = 34;
	/** RegularExpression Id. */
	int EXTEND = 35;
	/** RegularExpression Id. */
	int FN_OR = 36;
	/** RegularExpression Id. */
	int FUZZYTERM = 37;
	/** RegularExpression Id. */
	int MAXGAPS = 38;
	/** RegularExpression Id. */
	int MAXWIDTH = 39;
	/** RegularExpression Id. */
	int NON_OVERLAPPING = 40;
	/** RegularExpression Id. */
	int NOT_CONTAINED_BY = 41;
	/** RegularExpression Id. */
	int NOT_CONTAINING = 42;
	/** RegularExpression Id. */
	int NOT_WITHIN = 43;
	/** RegularExpression Id. */
	int ORDERED = 44;
	/** RegularExpression Id. */
	int OVERLAPPING = 45;
	/** RegularExpression Id. */
	int PHRASE = 46;
	/** RegularExpression Id. */
	int UNORDERED = 47;
	/** RegularExpression Id. */
	int UNORDERED_NO_OVERLAPS = 48;
	/** RegularExpression Id. */
	int WILDCARD = 49;
	/** RegularExpression Id. */
	int WITHIN = 50;
	/** RegularExpression Id. */
	int RANGE_TO = 51;
	/** RegularExpression Id. */
	int RANGEIN_END = 52;
	/** RegularExpression Id. */
	int RANGEEX_END = 53;
	/** RegularExpression Id. */
	int RANGE_QUOTED = 54;
	/** RegularExpression Id. */
	int RANGE_GOOP = 55;

	/** Lexical state. */
	int Function = 0;
	/** Lexical state. */
	int Range = 1;
	/** Lexical state. */
	int DEFAULT = 2;

	/** Literal token values. */
	String[] tokenImage = { "<EOF>", "<_NUM_CHAR>", "<_ESCAPED_CHAR>", "<_TERM_START_CHAR>", "<_TERM_CHAR>", "<_WHITESPACE>", "<_QUOTED_CHAR>",
			"<token of kind 7>", "<AND>", "<OR>", "<NOT>", "\"fn:\"", "\"+\"", "\"-\"", "\")\"", "\":\"", "\"=\"", "\"<\"", "\"<=\"", "\">\"", "\">=\"",
			"\"^\"", "\"~\"", "<QUOTED>", "<NUMBER>", "<TERM>", "<REGEXPTERM>", "\"[\"", "\"{\"", "\"(\"", "<ATLEAST>", "\"after\"", "\"before\"",
			"<CONTAINED_BY>", "\"containing\"", "\"extend\"", "\"or\"", "<FUZZYTERM>", "<MAXGAPS>", "<MAXWIDTH>", "<NON_OVERLAPPING>", "<NOT_CONTAINED_BY>",
			"<NOT_CONTAINING>", "<NOT_WITHIN>", "\"ordered\"", "\"overlapping\"", "\"phrase\"", "\"unordered\"", "<UNORDERED_NO_OVERLAPS>", "\"wildcard\"",
			"\"within\"", "\"TO\"", "\"]\"", "\"}\"", "<RANGE_QUOTED>", "<RANGE_GOOP>", "\"@\"", };

}
