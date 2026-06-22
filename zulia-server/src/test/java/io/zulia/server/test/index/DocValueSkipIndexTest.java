package io.zulia.server.test.index;

import io.zulia.ZuliaFieldConstants;
import io.zulia.message.ZuliaIndex.FieldConfig;
import io.zulia.message.ZuliaIndex.FieldConfig.FieldType;
import io.zulia.message.ZuliaIndex.IndexSettings;
import io.zulia.message.ZuliaIndex.SortAs;
import io.zulia.server.config.ServerIndexConfig;
import io.zulia.server.field.FieldTypeUtil;
import io.zulia.server.index.DocumentContainer;
import io.zulia.server.index.ShardDocumentIndexer;
import io.zulia.server.index.ZuliaIndexVersion;
import io.zulia.util.ZuliaUtil;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesSkipIndexType;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Verifies the per-field {@code docValueSkipIndex} flag actually controls whether Zulia writes a Lucene doc-values skip
 * index on the sort field. Drives {@link ShardDocumentIndexer} directly so the assertion is independent of the cluster stack.
 */
public class DocValueSkipIndexTest {

	private static final String SKIP_FIELD = "withSkip";
	private static final String NO_SKIP_FIELD = "noSkip";

	private static ShardDocumentIndexer indexer() {
		IndexSettings settings = IndexSettings.newBuilder()
				.setIndexName("dvSkip")
				.addFieldConfig(FieldConfig.newBuilder()
						.setStoredFieldName(SKIP_FIELD)
						.setFieldType(FieldType.NUMERIC_INT)
						.setDocValueSkipIndex(true)
						.addSortAs(SortAs.newBuilder().setSortFieldName(SKIP_FIELD)))
				.addFieldConfig(FieldConfig.newBuilder()
						.setStoredFieldName(NO_SKIP_FIELD)
						.setFieldType(FieldType.NUMERIC_INT)
						.addSortAs(SortAs.newBuilder().setSortFieldName(NO_SKIP_FIELD)))
				.build();
		return new ShardDocumentIndexer(new ServerIndexConfig(settings));
	}

	private static Document indexDocument(ShardDocumentIndexer indexer, DirectoryTaxonomyWriter taxoWriter) throws Exception {
		org.bson.Document mongoDocument = new org.bson.Document();
		mongoDocument.put(SKIP_FIELD, 5);
		mongoDocument.put(NO_SKIP_FIELD, 7);

		DocumentContainer mongoContainer = new DocumentContainer(ZuliaUtil.mongoDocumentToByteArray(mongoDocument));
		DocumentContainer metadataContainer = new DocumentContainer((byte[]) null);

		return indexer.getIndexDocument("1", 0L, mongoContainer, metadataContainer, taxoWriter);
	}

	@Test
	public void flagControlsSortFieldType() throws Exception {
		try (Directory taxoDir = new ByteBuffersDirectory(); DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir)) {
			Document luceneDocument = indexDocument(indexer(), taxoWriter);

			IndexableField skipField = luceneDocument.getField(FieldTypeUtil.getSortField(SKIP_FIELD, FieldType.NUMERIC_INT));
			IndexableField noSkipField = luceneDocument.getField(FieldTypeUtil.getSortField(NO_SKIP_FIELD, FieldType.NUMERIC_INT));

			Assertions.assertNotNull(skipField, "sort doc-values missing for " + SKIP_FIELD);
			Assertions.assertNotNull(noSkipField, "sort doc-values missing for " + NO_SKIP_FIELD);

			Assertions.assertEquals(DocValuesSkipIndexType.RANGE, skipField.fieldType().docValuesSkipIndexType());
			Assertions.assertEquals(DocValuesSkipIndexType.NONE, noSkipField.fieldType().docValuesSkipIndexType());
		}
	}

	@Test
	public void skipperPresentInSegmentOnlyWhenEnabled() throws Exception {
		String skipSortField = FieldTypeUtil.getSortField(SKIP_FIELD, FieldType.NUMERIC_INT);
		String noSkipSortField = FieldTypeUtil.getSortField(NO_SKIP_FIELD, FieldType.NUMERIC_INT);

		try (Directory dir = new ByteBuffersDirectory(); Directory taxoDir = new ByteBuffersDirectory()) {

			Document luceneDocument;
			try (DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir)) {
				luceneDocument = indexDocument(indexer(), taxoWriter);
				taxoWriter.commit();
			}

			try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
				writer.addDocument(luceneDocument);
				writer.commit();
			}

			try (DirectoryReader reader = DirectoryReader.open(dir)) {
				LeafReader leaf = reader.leaves().getFirst().reader();
				Assertions.assertNotNull(leaf.getDocValuesSkipper(skipSortField), "expected a doc-values skipper on " + skipSortField);
				Assertions.assertNull(leaf.getDocValuesSkipper(noSkipSortField), "did not expect a doc-values skipper on " + noSkipSortField);
			}
		}
	}

	@Test
	public void idSortFieldSkipperFollowsIndexVersion() throws Exception {
		String idSortField = FieldTypeUtil.getSortField(ZuliaFieldConstants.ID_SORT_FIELD, FieldType.STRING);

		// Legacy index (version 0): the built-in id sort field stays plain so its immutable schema is unchanged.
		assertIdSortSkipper(0, idSortField, false);

		// Index created at the feature version: the id sort field gets a doc-values skipper.
		assertIdSortSkipper(ZuliaIndexVersion.ID_SORT_DOC_VALUE_SKIP, idSortField, true);
	}

	private static void assertIdSortSkipper(int createdIndexVersion, String idSortField, boolean expectSkipper) throws Exception {
		IndexSettings settings = IndexSettings.newBuilder().setIndexName("dvSkipId").setCreatedIndexVersion(createdIndexVersion).build();
		ShardDocumentIndexer indexer = new ShardDocumentIndexer(new ServerIndexConfig(settings));

		try (Directory dir = new ByteBuffersDirectory(); Directory taxoDir = new ByteBuffersDirectory()) {
			Document luceneDocument;
			try (DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(taxoDir)) {
				luceneDocument = indexer.getIndexDocument("1", 0L, new DocumentContainer((byte[]) null), new DocumentContainer((byte[]) null), taxoWriter);
			}

			try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
				writer.addDocument(luceneDocument);
				writer.commit();
			}

			try (DirectoryReader reader = DirectoryReader.open(dir)) {
				LeafReader leaf = reader.leaves().getFirst().reader();
				if (expectSkipper) {
					Assertions.assertNotNull(leaf.getDocValuesSkipper(idSortField), "expected id sort skipper at version " + createdIndexVersion);
				}
				else {
					Assertions.assertNull(leaf.getDocValuesSkipper(idSortField), "did not expect id sort skipper at version " + createdIndexVersion);
				}
			}
		}
	}
}
