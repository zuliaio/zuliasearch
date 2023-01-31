package io.zulia.server.analysis;

import io.zulia.ZuliaConstants;
import io.zulia.message.ZuliaIndex;
import io.zulia.server.analysis.analyzer.BooleanAnalyzer;
import io.zulia.server.analysis.filter.BritishUSFilter;
import io.zulia.server.analysis.filter.CaseProtectedWordsFilter;
import io.zulia.server.config.ServerIndexConfig;
import io.zulia.server.field.FieldTypeUtil;
import org.apache.lucene.analysis.LowerCaseFilter;
import org.apache.lucene.analysis.StopFilter;
import org.apache.lucene.analysis.*;
import org.apache.lucene.analysis.core.*;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.en.EnglishMinimalStemFilter;
import org.apache.lucene.analysis.en.EnglishPossessiveFilter;
import org.apache.lucene.analysis.en.KStemFilter;
import org.apache.lucene.analysis.minhash.MinHashFilterFactory;
import org.apache.lucene.analysis.miscellaneous.ASCIIFoldingFilter;
import org.apache.lucene.analysis.miscellaneous.WordDelimiterGraphFilter;
import org.apache.lucene.analysis.shingle.ShingleFilter;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.lucene.analysis.miscellaneous.WordDelimiterGraphFilter.CATENATE_ALL;

public class ZuliaPerFieldAnalyzer extends DelegatingAnalyzerWrapper {
    private static final Logger LOG = Logger.getLogger(ZuliaPerFieldAnalyzer.class.getName());

    private final ServerIndexConfig indexConfig;
    private final Analyzer defaultAnalyzer;

    private Map<String, Analyzer> fieldAnalyzers;

    public ZuliaPerFieldAnalyzer(ServerIndexConfig indexConfig) {
        super(PER_FIELD_REUSE_STRATEGY);
        this.indexConfig = indexConfig;
        this.defaultAnalyzer = new KeywordAnalyzer();
        this.fieldAnalyzers = new HashMap<>();
        refresh();
    }

    public void refresh() {
        Map<String, Analyzer> newFieldAnalyzers = new HashMap<>();
        for (ZuliaIndex.IndexAs indexAs : indexConfig.getIndexAsValues()) {
            String indexFieldName = indexAs.getIndexFieldName();

            ZuliaIndex.FieldConfig.FieldType fieldType = indexConfig.getFieldTypeForIndexField(indexFieldName);
            ZuliaIndex.AnalyzerSettings analyzerSettings = indexConfig.getAnalyzerSettingsForIndexField(indexFieldName);

            Analyzer a;

            if (ZuliaIndex.FieldConfig.FieldType.STRING.equals(fieldType)) {
                if (analyzerSettings != null) {
                    a = getAnalyzerForField(analyzerSettings);
                } else {
                    a = new KeywordAnalyzer();
                }
                newFieldAnalyzers.put(ZuliaConstants.CHAR_LENGTH_PREFIX + indexFieldName, new WhitespaceAnalyzer());
            } else if (ZuliaIndex.FieldConfig.FieldType.BOOL.equals(fieldType)) {
                a = new BooleanAnalyzer();
            } else if (FieldTypeUtil.isNumericOrDateFieldType(fieldType)) {
                a = new WhitespaceAnalyzer();
            } else {
                a = new KeywordAnalyzer();
            }

            newFieldAnalyzers.put(indexFieldName, a);
            newFieldAnalyzers.put(ZuliaConstants.LIST_LENGTH_PREFIX + indexFieldName, new WhitespaceAnalyzer());

        }

        this.fieldAnalyzers = newFieldAnalyzers;
    }

    @Override
    protected Analyzer getWrappedAnalyzer(String fieldName) {
        Analyzer analyzer = fieldAnalyzers.get(fieldName);
        return (analyzer != null) ? analyzer : defaultAnalyzer;
    }

    @Override
    public String toString() {
        return "ZuliaPerFieldAnalyzerWrapper(" + fieldAnalyzers + ", default=" + defaultAnalyzer + ")";
    }

    public static Analyzer getAnalyzerForField(ZuliaIndex.AnalyzerSettings analyzerSettings) {

        return new Analyzer() {

            @Override
            public int getPositionIncrementGap(String fieldName) {
                return 100;
            }

            @Override
            protected TokenStreamComponents createComponents(String fieldName) {

                ZuliaIndex.AnalyzerSettings.Tokenizer tokenizer = analyzerSettings.getTokenizer();
                Tokenizer src;
                if (ZuliaIndex.AnalyzerSettings.Tokenizer.KEYWORD.equals(tokenizer)) {
                    src = new KeywordTokenizer();
                } else if (ZuliaIndex.AnalyzerSettings.Tokenizer.WHITESPACE.equals(tokenizer)) {
                    src = new WhitespaceTokenizer();
                } else if (ZuliaIndex.AnalyzerSettings.Tokenizer.STANDARD.equals(tokenizer)) {
                    src = new StandardTokenizer();
                } else {
                    throw new RuntimeException("Unknown tokenizer type <" + tokenizer);
                }

                return new TokenStreamComponents(src, getFilteredStream(src));
            }

            @Override
            protected TokenStream normalize(String fieldName, TokenStream in) {
                return getFilteredStream(in);
            }

            private TokenStream getFilteredStream(TokenStream src) {
                TokenStream tok = src;
                TokenStream lastTok = src;

                List<ZuliaIndex.AnalyzerSettings.Filter> filterList = analyzerSettings.getFilterList();
                for (ZuliaIndex.AnalyzerSettings.Filter filter : filterList) {
                    if (ZuliaIndex.AnalyzerSettings.Filter.LOWERCASE.equals(filter)) {
                        tok = new LowerCaseFilter(lastTok);
                    } else if (ZuliaIndex.AnalyzerSettings.Filter.UPPERCASE.equals(filter)) {
                        tok = new UpperCaseFilter(lastTok);
                    } else if (ZuliaIndex.AnalyzerSettings.Filter.ASCII_FOLDING.equals(filter)) {
                        tok = new ASCIIFoldingFilter(lastTok);
                    } else if (ZuliaIndex.AnalyzerSettings.Filter.TWO_TWO_SHINGLE.equals(filter)) {
                        ShingleFilter shingleFilter = new ShingleFilter(lastTok, 2, 2);
                        shingleFilter.setOutputUnigrams(false);
                        tok = shingleFilter;
                    } else if (ZuliaIndex.AnalyzerSettings.Filter.THREE_THREE_SHINGLE.equals(filter)) {
                        ShingleFilter shingleFilter = new ShingleFilter(lastTok, 3, 3);
                        shingleFilter.setOutputUnigrams(false);
                        tok = shingleFilter;
                    } else if (ZuliaIndex.AnalyzerSettings.Filter.FOUR_FOUR_SHINGLE.equals(filter)) {
                        ShingleFilter shingleFilter = new ShingleFilter(lastTok, 4, 4);
                        shingleFilter.setOutputUnigrams(false);
                        tok = shingleFilter;
                    } else if (ZuliaIndex.AnalyzerSettings.Filter.FIVE_FIVE_SHINGLE.equals(filter)) {
                        ShingleFilter shingleFilter = new ShingleFilter(lastTok, 5, 5);
                        shingleFilter.setOutputUnigrams(false);
                        tok = shingleFilter;
                    } else if (ZuliaIndex.AnalyzerSettings.Filter.KSTEM.equals(filter)) {
                        tok = new KStemFilter(lastTok);
                    } else if (ZuliaIndex.AnalyzerSettings.Filter.STOPWORDS.equals(filter)) {
                        CharArraySet stopWordsSet = EnglishAnalyzer.ENGLISH_STOP_WORDS_SET;

                        File file = new File(System.getProperty("user.home") + File.separator + ".zulia" + File.separator + "stopwords.txt");
                        if (file.exists()) {
                            try {
                                try (Stream<String> lines = Files.lines(file.toPath())) {
                                    List<String> fileLines = lines.map(String::trim).collect(Collectors.toList());
                                    stopWordsSet = StopFilter.makeStopSet(fileLines);
                                }
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }

                        tok = new StopFilter(lastTok, stopWordsSet);
                    } else if (ZuliaIndex.AnalyzerSettings.Filter.ENGLISH_MIN_STEM.equals(filter)) {
                        tok = new EnglishMinimalStemFilter(lastTok);
                    } else if (ZuliaIndex.AnalyzerSettings.Filter.SNOWBALL_STEM.equals(filter)) {
                        tok = new SnowballFilter(lastTok, "English");
                    } else if (ZuliaIndex.AnalyzerSettings.Filter.ENGLISH_POSSESSIVE.equals(filter)) {
                        tok = new EnglishPossessiveFilter(lastTok);
                    } else if (ZuliaIndex.AnalyzerSettings.Filter.MINHASH.equals(filter)) {
                        tok = new MinHashFilterFactory(Collections.emptyMap()).create(lastTok);
                    } else if (ZuliaIndex.AnalyzerSettings.Filter.BRITISH_US.equals(filter)) {
                        tok = new BritishUSFilter(lastTok);
                    } else if (ZuliaIndex.AnalyzerSettings.Filter.CONCAT_ALL.equals(filter)) {
                        tok = new WordDelimiterGraphFilter(lastTok, CATENATE_ALL, null);
                    } else if (ZuliaIndex.AnalyzerSettings.Filter.CASE_PROTECTED_WORDS.equals(filter)) {
                        tok = new CaseProtectedWordsFilter(lastTok);
                    } else {
                        throw new RuntimeException("Unknown filter type <" + filter + ">");
                    }
                    lastTok = tok;
                }
                return tok;
            }

        };

    }

}
