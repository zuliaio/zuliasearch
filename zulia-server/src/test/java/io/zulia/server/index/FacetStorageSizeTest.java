package io.zulia.server.index;

import io.zulia.ZuliaFieldConstants;
import io.zulia.client.config.ClientIndexConfig;
import io.zulia.fields.FacetConfigBuilder;
import io.zulia.fields.FieldConfigBuilder;
import io.zulia.server.config.ServerIndexConfig;
import io.zulia.util.ZuliaUtil;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.taxonomy.FacetLabel;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.util.BytesRef;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.List;

/**
 * Each facet storage doc value holds exactly its dims and their ordinals, so a facet group value carries no bytes for facets outside the group.
 */
public class FacetStorageSizeTest {

	@Test
	public void facetValuesAreSizedToTheirContent() throws Exception {
		ClientIndexConfig config = new ClientIndexConfig();
		config.setIndexName("facetStorageSize");
		config.addFieldConfig(FieldConfigBuilder.createString("a").facetAs(FacetConfigBuilder.create().withGroups("g1")));
		config.addFieldConfig(FieldConfigBuilder.createString("b").facetAs(FacetConfigBuilder.create().withGroups("g1")));
		// in g1 but absent from the document, so it is written as the dim with a zero count
		config.addFieldConfig(FieldConfigBuilder.createString("e").facetAs(FacetConfigBuilder.create().withGroups("g1")));
		config.addFieldConfig(FieldConfigBuilder.createString("c").facetAs(FacetConfigBuilder.create().withGroups("g2")));
		// in no group, its ordinals used to pad every group value
		config.addFieldConfig(FieldConfigBuilder.createString("d").facet());

		org.bson.Document source = new org.bson.Document("id", "1").append("a", List.of("x", "y")).append("b", "z").append("c", "w")
				.append("d", List.of("p", "q", "r"));

		ShardDocumentIndexer indexer = new ShardDocumentIndexer(new ServerIndexConfig(config.getIndexSettings()));
		try (DirectoryTaxonomyWriter taxoWriter = new DirectoryTaxonomyWriter(new ByteBuffersDirectory())) {
			Document document = indexer.getIndexDocument("1", 0L, new DocumentContainer(ZuliaUtil.mongoDocumentToByteArray(source)),
					new DocumentContainer((byte[]) null), taxoWriter);

			// g1: dims a, b, e with 2 + 1 + 0 ordinals
			assertValue(document, ZuliaFieldConstants.FACET_STORAGE_GROUP + "g1", taxoWriter, List.of("a", "b", "e"), 3);
			// g2: dim c with 1 ordinal
			assertValue(document, ZuliaFieldConstants.FACET_STORAGE_GROUP + "g2", taxoWriter, List.of("c"), 1);
			// default storage: every dim the document has, a, b, c, d with 2 + 1 + 1 + 3 ordinals
			assertValue(document, ZuliaFieldConstants.FACET_STORAGE, taxoWriter, List.of("a", "b", "c", "d"), 7);
		}
	}

	private static void assertValue(Document document, String field, DirectoryTaxonomyWriter taxoWriter, List<String> dims, int ordinalCount) throws Exception {
		IndexableField indexableField = document.getField(field);
		Assertions.assertNotNull(indexableField, "missing doc value " + field);
		BytesRef value = indexableField.binaryValue();
		Assertions.assertEquals((2 * dims.size() + ordinalCount) * 4, value.length, "length of " + field);

		IntBuffer ints = ByteBuffer.wrap(value.bytes, value.offset, value.length).asIntBuffer();
		int[] expectedDims = dims.stream().mapToInt(dim -> ordinalOf(taxoWriter, dim)).sorted().toArray();
		int ordinalsRead = 0;
		for (int expectedDim : expectedDims) {
			Assertions.assertEquals(expectedDim, ints.get(), "dim ordinal in " + field);
			int count = ints.get();
			for (int i = 0; i < count; i++) {
				ints.get();
			}
			ordinalsRead += count;
		}
		Assertions.assertEquals(ordinalCount, ordinalsRead, "ordinals in " + field);
		Assertions.assertFalse(ints.hasRemaining(), "bytes left after the last dim in " + field);
	}

	private static int ordinalOf(DirectoryTaxonomyWriter taxoWriter, String dim) {
		try {
			return taxoWriter.addCategory(new FacetLabel(dim));
		}
		catch (Exception e) {
			throw new RuntimeException(e);
		}
	}
}
