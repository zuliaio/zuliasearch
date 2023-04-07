package io.zulia.server.index;

import com.google.protobuf.ByteString;
import io.zulia.ZuliaFieldConstants;
import io.zulia.message.ZuliaBase;
import io.zulia.message.ZuliaIndex;
import io.zulia.message.ZuliaQuery;
import io.zulia.server.analysis.highlight.ZuliaHighlighter;
import io.zulia.server.field.FieldTypeUtil;
import io.zulia.server.util.BytesRefUtil;
import io.zulia.server.util.FieldAndSubFields;
import io.zulia.util.ResultHelper;
import io.zulia.util.ZuliaUtil;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.highlight.TextFragment;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static io.zulia.ZuliaFieldConstants.STORED_DOC_FIELD;
import static io.zulia.ZuliaFieldConstants.STORED_ID_FIELD;
import static io.zulia.ZuliaFieldConstants.STORED_META_FIELD;

public class DocumentScoredDocLeafHandler extends ScoredDocLeafHandler<ZuliaQuery.ScoredResult> {
	private BinaryDocValues idDocValues;

	private BinaryDocValues metaDocValues;

	private BinaryDocValues fullDocValues;

	private final String indexName;
	private final int shardNumber;
	private final boolean meta;
	private final boolean full;
	private final boolean needsDocFiltering;

	private final boolean needsHighlight;

	private final boolean needsAnalysis;

	private final List<String> fieldsToReturn;
	private final List<String> fieldsToMask;
	private final List<SortMeta> sortMetas;
	private final List<ZuliaHighlighter> highlighterList;
	private final List<AnalysisHandler> analysisHandlerList;

	public DocumentScoredDocLeafHandler(String indexName, int shardNumber, ZuliaQuery.FetchType fetchType, List<String> fieldsToReturn,
			List<String> fieldsToMask, List<SortMeta> sortMetas, List<ZuliaHighlighter> highlighterList, List<AnalysisHandler> analysisHandlerList) {

		this.indexName = indexName;
		this.shardNumber = shardNumber;
		meta = ZuliaQuery.FetchType.META.equals(fetchType) || ZuliaQuery.FetchType.ALL.equals(fetchType);
		full = ZuliaQuery.FetchType.FULL.equals(fetchType) || ZuliaQuery.FetchType.ALL.equals(fetchType);
		this.fieldsToReturn = fieldsToReturn;
		this.fieldsToMask = fieldsToMask;
		this.highlighterList = highlighterList;
		this.analysisHandlerList = analysisHandlerList;
		this.needsHighlight = !highlighterList.isEmpty();
		this.needsAnalysis = !analysisHandlerList.isEmpty();
		this.sortMetas = sortMetas;
		this.needsDocFiltering = !fieldsToMask.isEmpty() || !fieldsToReturn.isEmpty();

	}

	@Override
	protected void handleNewLeaf(LeafReaderContext currentLeaf) throws IOException {
		LeafReader leafReader = currentLeaf.reader();

		idDocValues = leafReader.getBinaryDocValues(STORED_ID_FIELD);

		if (meta) {
			metaDocValues = leafReader.getBinaryDocValues(STORED_META_FIELD);
		}

		if (full) {
			fullDocValues = leafReader.getBinaryDocValues(STORED_DOC_FIELD);
		}
	}

	@Override
	protected ZuliaQuery.ScoredResult handleDocument(LeafReaderContext currentLeaf, int docId, ScoreDoc scoreDoc) throws IOException {

		ZuliaQuery.ScoredResult.Builder srBuilder = ZuliaQuery.ScoredResult.newBuilder();
		srBuilder.setScore(scoreDoc.score);
		srBuilder.setLuceneShardId(docId);
		srBuilder.setIndexName(indexName);
		srBuilder.setShard(shardNumber);

		ZuliaBase.IdInfo idInfo;
		if (idDocValues.advanceExact(docId)) {
			byte[] idInfoBytes = BytesRefUtil.getByteArray(idDocValues.binaryValue());

			idInfo = ZuliaBase.IdInfo.parseFrom(idInfoBytes);
			srBuilder.setUniqueId(idInfo.getId());
			srBuilder.setTimestamp(idInfo.getTimestamp());
		}
		else {
			throw new IOException("Failed to parse id field for document with lucene id <" + docId + ">");
		}

		if (meta || full) {
			ZuliaBase.ResultDocument.Builder rdBuilder = ZuliaBase.ResultDocument.newBuilder();
			//TODO why is this duplicated from the scored result
			rdBuilder.setIndexName(indexName);
			rdBuilder.setUniqueId(idInfo.getId());
			rdBuilder.setTimestamp(idInfo.getTimestamp());
			if (meta) {
				if (metaDocValues != null && metaDocValues.advanceExact(docId)) {
					byte[] metaBytes = BytesRefUtil.getByteArray(metaDocValues.binaryValue());
					rdBuilder.setMetadata(ByteString.copyFrom(metaBytes));
				}
			}

			if (full) {
				if (fullDocValues != null && fullDocValues.advanceExact(docId)) {
					byte[] docBytes = BytesRefUtil.getByteArray(fullDocValues.binaryValue());
					rdBuilder.setDocument(ByteString.copyFrom(docBytes));

					if (needsHighlight || needsAnalysis || needsDocFiltering) {
						org.bson.Document mongoDoc = ResultHelper.getDocumentFromResultDocument(rdBuilder);
						if (mongoDoc != null) {
							if (needsHighlight) {
								handleHighlight(highlighterList, srBuilder, mongoDoc);
							}
							if (needsAnalysis) {
								AnalysisHandler.handleDocument(mongoDoc, analysisHandlerList, srBuilder);
							}

							if (needsDocFiltering) {
								filterDocument(fieldsToReturn, fieldsToMask, mongoDoc);
								ByteString document = ZuliaUtil.mongoDocumentToByteString(mongoDoc);
								rdBuilder.setDocument(document);
							}
						}
					}
				}

			}
			srBuilder.setResultDocument(rdBuilder);

		}

		if (!sortMetas.isEmpty()) {
			handleSortValues(sortMetas, scoreDoc, srBuilder);
		}

		return srBuilder.build();
	}

	private void filterDocument(Collection<String> fieldsToReturn, Collection<String> fieldsToMask, org.bson.Document mongoDocument) {

		if (fieldsToReturn.isEmpty() && !fieldsToMask.isEmpty()) {
			FieldAndSubFields fieldsToMaskObj = new FieldAndSubFields(fieldsToMask);
			for (String topLevelField : fieldsToMaskObj.getTopLevelFields()) {
				Map<String, Set<String>> topLevelToChildren = fieldsToMaskObj.getTopLevelToChildren();
				if (!topLevelToChildren.containsKey(topLevelField)) {
					mongoDocument.remove(topLevelField);
				}
				else {
					Object subDoc = mongoDocument.get(topLevelField);
					ZuliaUtil.handleLists(subDoc, subDocItem -> {
						if (subDocItem instanceof org.bson.Document) {

							Collection<String> subFieldsToMask =
									topLevelToChildren.get(topLevelField) != null ? topLevelToChildren.get(topLevelField) : Collections.emptyList();

							filterDocument(Collections.emptyList(), subFieldsToMask, (org.bson.Document) subDocItem);
						}
						else if (subDocItem == null) {

						}
						else {
							//TODO: warn user?
						}
					});
				}
			}
		}
		else if (!fieldsToReturn.isEmpty()) {
			FieldAndSubFields fieldsToReturnObj = new FieldAndSubFields(fieldsToReturn);
			FieldAndSubFields fieldsToMaskObj = new FieldAndSubFields(fieldsToMask);

			Set<String> topLevelFieldsToReturn = fieldsToReturnObj.getTopLevelFields();
			Set<String> topLevelFieldsToMask = fieldsToMaskObj.getTopLevelFields();
			Map<String, Set<String>> topLevelToChildrenToMask = fieldsToMaskObj.getTopLevelToChildren();
			Map<String, Set<String>> topLevelToChildrenToReturn = fieldsToReturnObj.getTopLevelToChildren();

			ArrayList<String> allDocumentKeys = new ArrayList<>(mongoDocument.keySet());
			for (String topLevelField : allDocumentKeys) {

				if ((!topLevelFieldsToReturn.contains(topLevelField) && !topLevelToChildrenToMask.containsKey(topLevelField))
						|| topLevelFieldsToMask.contains(topLevelField) && !topLevelToChildrenToMask.containsKey(topLevelField)) {
					mongoDocument.remove(topLevelField);
				}

				if (topLevelToChildrenToReturn.containsKey(topLevelField) || topLevelToChildrenToMask.containsKey(topLevelField)) {
					Object subDoc = mongoDocument.get(topLevelField);
					ZuliaUtil.handleLists(subDoc, subDocItem -> {
						if (subDocItem instanceof org.bson.Document) {

							Collection<String> subFieldsToReturn = topLevelToChildrenToReturn.get(topLevelField) != null ?
									topLevelToChildrenToReturn.get(topLevelField) :
									Collections.emptyList();

							Collection<String> subFieldsToMask =
									topLevelToChildrenToMask.get(topLevelField) != null ? topLevelToChildrenToMask.get(topLevelField) : Collections.emptyList();

							filterDocument(subFieldsToReturn, subFieldsToMask, (org.bson.Document) subDocItem);
						}
						else if (subDocItem == null) {

						}
						else {
							//TODO: warn user?
						}
					});

				}
			}
		}

	}

	private void handleSortValues(List<SortMeta> sortMetas, ScoreDoc scoreDoc, ZuliaQuery.ScoredResult.Builder srBuilder) {
		FieldDoc result = (FieldDoc) scoreDoc;

		ZuliaQuery.SortValues.Builder sortValues = ZuliaQuery.SortValues.newBuilder();

		int c = 0;

		for (Object o : result.fields) {
			if (o == null) {
				sortValues.addSortValue(ZuliaQuery.SortValue.newBuilder().setExists(false));
				c++;
				continue;
			}

			SortMeta sortMeta = sortMetas.get(c);
			String sortField = sortMeta.sortField();

			if (ZuliaFieldConstants.SCORE_FIELD.equals(sortField)) {
				sortValues.addSortValue(ZuliaQuery.SortValue.newBuilder().setFloatValue(scoreDoc.score));
				c++;
				continue;
			}

			ZuliaIndex.FieldConfig.FieldType fieldType = sortMeta.sortFieldType();

			ZuliaQuery.SortValue.Builder sortValueBuilder = ZuliaQuery.SortValue.newBuilder().setExists(true);
			if (FieldTypeUtil.isHandledAsNumericFieldType(fieldType)) {
				if (FieldTypeUtil.isStoredAsInt(fieldType)) {
					sortValueBuilder.setIntegerValue((Integer) o);
				}
				else if (FieldTypeUtil.isNumericLongFieldType(fieldType)) {
					sortValueBuilder.setLongValue((Long) o);
				}
				else if (FieldTypeUtil.isDateFieldType(fieldType)) {
					//TODO should this just use the LongValue and isStoredAsLong(fieldType) above
					sortValueBuilder.setDateValue((Long) o);
				}
				else if (FieldTypeUtil.isNumericFloatFieldType(fieldType)) {
					sortValueBuilder.setFloatValue((Float) o);
				}
				else if (FieldTypeUtil.isNumericDoubleFieldType(fieldType)) {
					sortValueBuilder.setDoubleValue((Double) o);
				}

			}
			else {
				BytesRef b = (BytesRef) o;
				sortValueBuilder.setStringValue(b.utf8ToString());
			}
			sortValues.addSortValue(sortValueBuilder);

			c++;
		}
		srBuilder.setSortValues(sortValues);
	}

	private void handleHighlight(List<ZuliaHighlighter> highlighterList, ZuliaQuery.ScoredResult.Builder srBuilder, org.bson.Document doc) {

		for (ZuliaHighlighter highlighter : highlighterList) {

			String storedFieldName = highlighter.getStoredFieldName();

			if (storedFieldName != null) {
				ZuliaQuery.HighlightResult.Builder highLightResult = ZuliaQuery.HighlightResult.newBuilder();

				highLightResult.setField(storedFieldName);

				Object storeFieldValues = ResultHelper.getValueFromMongoDocument(doc, storedFieldName);

				ZuliaUtil.handleLists(storeFieldValues, (value) -> {
					String content = value.toString();

					try (TokenStream tokenStream = highlighter.getTokenStream(content)) {
						TextFragment[] bestTextFragments = highlighter.getBestTextFragments(tokenStream, content, false, highlighter.getNumberOfFragments());
						for (TextFragment bestTextFragment : bestTextFragments) {
							if (bestTextFragment != null && bestTextFragment.getScore() > 0) {
								highLightResult.addFragments(bestTextFragment.toString());
							}
						}
					}
					catch (Exception e) {
						throw new RuntimeException(e);
					}

				});

				srBuilder.addHighlightResult(highLightResult);
			}

		}

	}
}
