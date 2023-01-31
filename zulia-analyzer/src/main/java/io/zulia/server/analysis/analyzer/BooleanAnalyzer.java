package io.zulia.server.analysis.analyzer;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.KeywordTokenizer;
import org.apache.lucene.analysis.miscellaneous.KeepWordFilter;
import org.apache.lucene.analysis.pattern.PatternReplaceFilter;

import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * Created by Matt Davis on 2/10/16.
 */
public class BooleanAnalyzer extends Analyzer {

	public static final String TRUE_TOKEN = "T";
	public static final String FALSE_TOKEN = "F";

	private static final CharArraySet booleanTokens = CharArraySet.unmodifiableSet(new CharArraySet(Arrays.asList(TRUE_TOKEN, FALSE_TOKEN), false));
	public static final Pattern truePattern = Pattern.compile("true|t|yes|y|1", Pattern.CASE_INSENSITIVE);
	public static final Pattern falsePattern = Pattern.compile("false|f|no|n|0", Pattern.CASE_INSENSITIVE);

	@Override
	protected TokenStreamComponents createComponents(String fieldName) {

		final Tokenizer tokenizer = new KeywordTokenizer();
		TokenFilter result = new PatternReplaceFilter(tokenizer, truePattern, BooleanAnalyzer.TRUE_TOKEN, false);
		result = new PatternReplaceFilter(result, falsePattern, FALSE_TOKEN, false);
		result = new KeepWordFilter(result, booleanTokens);

		return new TokenStreamComponents(tokenizer, result);

	}

}
