package io.zulia.server.index;

import io.zulia.message.ZuliaBase.Term;
import io.zulia.message.ZuliaQuery.AnalysisRequest;
import io.zulia.message.ZuliaQuery.AnalysisResult;
import io.zulia.message.ZuliaQuery.ScoredResult;
import io.zulia.server.analysis.frequency.DocFreq;
import io.zulia.server.analysis.frequency.TermFreq;
import io.zulia.server.config.ServerIndexConfig;
import io.zulia.util.ResultHelper;
import io.zulia.util.ZuliaUtil;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.bson.Document;

import java.util.List;

/**
 * Created by Matt Davis on 6/29/16.
 *
 * @author mdavis
 */
public class AnalysisHandler {
    private final AnalysisRequest analysisRequest;
    private final String indexField;
    private final String storedFieldName;
    private final Analyzer analyzer;

    private final boolean computeDocLevel;
    private final boolean summaryLevelEnabled;
    private final boolean enabled;
    private final int minWordLength;
    private final int maxWordLength;
    private final AnalysisRequest.SummaryType summaryType;

    private Integer minShardDocFreqCount;
    private Integer maxShardDocFreqCount;

    private DocFreq docFreq;
    private TermFreq summaryTermFreq;

    public AnalysisHandler(ShardReader shardReader, Analyzer analyzer, ServerIndexConfig indexConfig, AnalysisRequest analysisRequest) {
        this.analysisRequest = analysisRequest;
        this.indexField = analysisRequest.getField();
        this.storedFieldName = indexConfig.getStoredFieldName(indexField);
        this.analyzer = analyzer;

        this.summaryType = analysisRequest.getSummaryType();
        this.computeDocLevel = analysisRequest.getDocTerms() || analysisRequest.getTokens() || AnalysisRequest.SummaryType.TOP_TERMS_TOP_N.equals(summaryType);
        this.summaryLevelEnabled = analysisRequest.getSummaryTerms();

        this.enabled = computeDocLevel || summaryLevelEnabled;

        this.minWordLength = analysisRequest.getMinWordLen();
        this.maxWordLength = analysisRequest.getMaxWordLen();

        boolean needDocFreq = (analysisRequest.getMinShardFreqPerc() > 0 || analysisRequest.getMinShardFreqPerc() > 0 || analysisRequest.getMinShardFreq() > 0
                || analysisRequest.getMaxShardFreq() > 0 || AnalysisRequest.TermSort.TFIDF.equals(analysisRequest.getTermSort()));

        if (needDocFreq) {
            this.docFreq = new DocFreq(shardReader, analysisRequest.getField());
            if (analysisRequest.getMinShardFreqPerc() != 0) {
                this.minShardDocFreqCount = docFreq.getNumDocsForPercent(analysisRequest.getMinShardFreqPerc());
            }
            if (analysisRequest.getMaxShardFreqPerc() != 0) {
                this.maxShardDocFreqCount = docFreq.getNumDocsForPercent(analysisRequest.getMaxShardFreqPerc());
            }

            if (analysisRequest.getMinShardFreq() != 0) {
                this.minShardDocFreqCount = analysisRequest.getMinShardFreq();
            }

            if (analysisRequest.getMaxShardFreq() != 0) {
                this.maxShardDocFreqCount = analysisRequest.getMaxShardFreq();
            }

        }

        if (summaryLevelEnabled) {
            this.summaryTermFreq = new TermFreq(docFreq);
        }
    }

    public static void handleDocument(org.bson.Document doc, List<AnalysisHandler> analysisHandlerList, ScoredResult.Builder srBuilder) {
        for (AnalysisHandler analysisHandler : analysisHandlerList) {
            AnalysisResult analysisResult = analysisHandler.handleDocument(doc);
            if (analysisResult != null) {
                srBuilder.addAnalysisResult(analysisResult);
            }
        }
    }

    public AnalysisResult handleDocument(Document document) {

        if (storedFieldName != null && enabled) {

            Object storeFieldValues = ResultHelper.getValueFromMongoDocument(document, storedFieldName);

            AnalysisResult.Builder analysisResult = AnalysisResult.newBuilder();
            analysisResult.setAnalysisRequest(analysisRequest);

            TermFreq docTermFreq = null;
            boolean needDocFreq = computeDocLevel || AnalysisRequest.SummaryType.TOP_TERMS_TOP_N.equals(summaryType);
            if (needDocFreq) {
                docTermFreq = new TermFreq(docFreq);
            }
            final TermFreq docTermFreqFinal = docTermFreq;

            ZuliaUtil.handleLists(storeFieldValues, (value) -> {
                String content = value.toString();
                try (TokenStream tokenStream = analyzer.tokenStream(indexField, content)) {
                    tokenStream.reset();
                    while (tokenStream.incrementToken()) {
                        String token = tokenStream.getAttribute(CharTermAttribute.class).toString();

                        if (analysisRequest.getTokens()) {
                            analysisResult.addToken(token);
                        }

                        if (minWordLength > 0) {
                            if (token.length() < minWordLength) {
                                continue;
                            }
                        }
                        if (maxWordLength > 0) {
                            if (token.length() > maxWordLength) {
                                continue;
                            }
                        }

                        if (maxShardDocFreqCount != null || minShardDocFreqCount != null) {
                            int termDocFreq = this.docFreq.getDocFreq(token);

                            if (minShardDocFreqCount != null) {
                                if (termDocFreq < minShardDocFreqCount) {
                                    continue;
                                }
                            }
                            if (maxShardDocFreqCount != null) {
                                if (termDocFreq > maxShardDocFreqCount) {
                                    continue;
                                }
                            }
                        }

                        if (needDocFreq) {
                            docTermFreqFinal.addTerm(token);
                        }
                        if (summaryLevelEnabled && AnalysisRequest.SummaryType.ALL_TERMS_TOP_N.equals(summaryType)) {
                            summaryTermFreq.addTerm(token);
                        }

                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

            });

            if (computeDocLevel) {
                List<Term.Builder> termBuilderList = docTermFreq.getTopTerms(analysisRequest.getTopN(), analysisRequest.getTermSort());
                if (analysisRequest.getDocTerms()) {
                    termBuilderList.forEach(analysisResult::addTerms);
                    return analysisResult.build();
                }
                if (summaryLevelEnabled && AnalysisRequest.SummaryType.TOP_TERMS_TOP_N.equals(summaryType)) {
                    termBuilderList.forEach(summaryTermFreq::addTerm);
                }

            }
            return null;

        }
        return null;
    }

    public AnalysisResult getShardResult() {
        if (summaryLevelEnabled) {
            AnalysisResult.Builder analysisResult = AnalysisResult.newBuilder();
            analysisResult.setAnalysisRequest(analysisRequest);

            //return all from shard for now
            int segmentTopN = 0;
            List<Term.Builder> termBuilderList = summaryTermFreq.getTopTerms(segmentTopN, analysisRequest.getTermSort());
            termBuilderList.forEach(analysisResult::addTerms);

            return analysisResult.build();
        }
        return null;
    }

}
