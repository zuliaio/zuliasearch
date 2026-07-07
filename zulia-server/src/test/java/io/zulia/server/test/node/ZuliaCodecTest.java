package io.zulia.server.test.node;

import io.zulia.message.ZuliaIndex.FieldConfig;
import io.zulia.message.ZuliaIndex.IndexAs;
import io.zulia.message.ZuliaIndex.IndexSettings;
import io.zulia.message.ZuliaIndex.VectorIndexingConfig;
import io.zulia.server.config.ServerIndexConfig;
import io.zulia.server.index.ZuliaIndexVersion;
import io.zulia.server.index.ZuliaLucene104Codec;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.codecs.lucene104.Lucene104HnswScalarQuantizedVectorsFormat;
import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Random;

/**
 * Proves quantization is applied at the codec level rather than silently falling back to float32, using the per-field KNN format
 * name in FieldInfos, and the on-disk size which grows with quantization bit width (the raw vectors are retained for rescoring).
 */
public class ZuliaCodecTest {

	private static final String FIELD = "v";
	private static final int DIMS = 256;
	private static final int DOCS = 500;

	private static final String HNSW_FLOAT_NAME = new Lucene99HnswVectorsFormat().getName();
	private static final String HNSW_QUANTIZED_NAME = new Lucene104HnswScalarQuantizedVectorsFormat().getName();
	private static final String FLAT_QUANTIZED_NAME = new Lucene104ScalarQuantizedVectorsFormat().getName();

	@Test
	public void formatNamePerEncoding() throws Exception {
		// a config-less field and an explicit FLOAT32 field both stay raw float32 HNSW
		Assertions.assertEquals(HNSW_FLOAT_NAME, generateIndexAndGetFormatName(buildConfig(null, false, 0), FIELD), "config-less vector field must stay float32");
		Assertions.assertEquals(HNSW_FLOAT_NAME, generateIndexAndGetFormatName(buildConfig(VectorIndexingConfig.Encoding.FLOAT32, false, 0), FIELD));

		for (VectorIndexingConfig.Encoding encoding : new VectorIndexingConfig.Encoding[] { VectorIndexingConfig.Encoding.INT8, VectorIndexingConfig.Encoding.INT7,
				VectorIndexingConfig.Encoding.INT4, VectorIndexingConfig.Encoding.BBQ, VectorIndexingConfig.Encoding.BBQ_2BIT }) {
			String name = generateIndexAndGetFormatName(buildConfig(encoding, false, 0), FIELD);
			Assertions.assertEquals(HNSW_QUANTIZED_NAME, name, encoding + " must use the quantized HNSW format, not " + name);
		}

		Assertions.assertEquals(FLAT_QUANTIZED_NAME, generateIndexAndGetFormatName(buildConfig(VectorIndexingConfig.Encoding.INT8, true, 0), FIELD));
	}

	@Test
	public void quantizedPayloadIsBitWidthProportional() throws Exception {
		long float32 = generateIndexAndGetSize(buildConfig(VectorIndexingConfig.Encoding.FLOAT32, false, 0));
		long bbq = generateIndexAndGetSize(buildConfig(VectorIndexingConfig.Encoding.BBQ, false, 0));
		long int4 = generateIndexAndGetSize(buildConfig(VectorIndexingConfig.Encoding.INT4, false, 0));
		long int8 = generateIndexAndGetSize(buildConfig(VectorIndexingConfig.Encoding.INT8, false, 0));

		// the quantized formats retain the raw float vectors for rescoring and add the quantized data on top, so
		// size grows with bit width, direct evidence each field wrote its configured encoding
		Assertions.assertTrue(bbq > float32, "BBQ (" + bbq + ") should add quantized data over FLOAT32 (" + float32 + ")");
		Assertions.assertTrue(int4 > bbq, "INT4 (" + int4 + ") should be larger than BBQ (" + bbq + ")");
		Assertions.assertTrue(int8 > int4, "INT8 (" + int8 + ") should be larger than INT4 (" + int4 + ")");
	}

	@Test
	public void versionedCodecBindingIsPinned() {
		// the persisted name and Lucene delegate are the on-disk contract for existing segments. These fail loudly
		// if the Zulia104 to Lucene104 binding ever changes, keeping upgrades a deliberate add-a-new-codec decision
		ZuliaLucene104Codec codec = new ZuliaLucene104Codec();
		Assertions.assertEquals("Zulia104", codec.getName(), "Renaming the codec would make existing Zulia104 indexes unreadable");
		Lucene104Codec delegate = new Lucene104Codec();
		Assertions.assertEquals(delegate.segmentInfoFormat().getClass(), codec.segmentInfoFormat().getClass(),
				"Zulia104 must keep delegating to Lucene104 formats; add a new ZuliaLuceneNNNCodec for a newer Lucene default");
		Assertions.assertEquals(delegate.storedFieldsFormat().getClass(), codec.storedFieldsFormat().getClass(),
				"Zulia104 must keep delegating to Lucene104 formats; add a new ZuliaLuceneNNNCodec for a newer Lucene default");
	}

	@Test
	public void configlessFieldFollowsIndexVersion() throws Exception {
		// a config-less field inherits the index-creation-version default: float32 below VECTOR_DEFAULT_INT8, INT8 at/after
		Assertions.assertEquals(HNSW_FLOAT_NAME, generateIndexAndGetFormatName(buildConfig(null, false, ZuliaIndexVersion.VECTOR_DEFAULT_INT8 - 1), FIELD),
				"config-less field on a pre-vector-default index must stay float32");
		Assertions.assertEquals(HNSW_QUANTIZED_NAME, generateIndexAndGetFormatName(buildConfig(null, false, ZuliaIndexVersion.VECTOR_DEFAULT_INT8), FIELD),
				"config-less field on a current index must default to INT8 (quantized HNSW format)");
	}

	@Test
	public void perRepresentationFormats() throws Exception {
		// one stored field indexed under two names, each IndexAs carrying its own encoding
		FieldConfig fieldConfig = FieldConfig.newBuilder().setStoredFieldName(FIELD).setFieldType(FieldConfig.FieldType.VECTOR)
				.addIndexAs(IndexAs.newBuilder().setIndexFieldName("vExact")
						.setVectorIndexingConfig(VectorIndexingConfig.newBuilder().setEncoding(VectorIndexingConfig.Encoding.FLOAT32)))
				.addIndexAs(IndexAs.newBuilder().setIndexFieldName("vBBQ")
						.setVectorIndexingConfig(VectorIndexingConfig.newBuilder().setEncoding(VectorIndexingConfig.Encoding.BBQ))).build();
		IndexSettings indexSettings = IndexSettings.newBuilder().setIndexName("codecTest").setCreatedIndexVersion(ZuliaIndexVersion.CURRENT)
				.addFieldConfig(fieldConfig).build();
		ServerIndexConfig config = new ServerIndexConfig(indexSettings);

		try (Directory directory = new ByteBuffersDirectory()) {
			writeVectors(directory, config, "vExact", "vBBQ");
			try (DirectoryReader reader = DirectoryReader.open(directory)) {
				LeafReaderContext leaf = reader.leaves().getFirst();
				Assertions.assertEquals(HNSW_FLOAT_NAME, leaf.reader().getFieldInfos().fieldInfo("vExact").getAttribute(PerFieldKnnVectorsFormat.PER_FIELD_FORMAT_KEY),
						"the FLOAT32 representation must persist the raw float32 format");
				Assertions.assertEquals(HNSW_QUANTIZED_NAME, leaf.reader().getFieldInfos().fieldInfo("vBBQ").getAttribute(PerFieldKnnVectorsFormat.PER_FIELD_FORMAT_KEY),
						"the BBQ representation must persist the quantized format");
			}
		}
	}


	private static ServerIndexConfig buildConfig(VectorIndexingConfig.Encoding encoding, boolean flat, int createdIndexVersion) {
		IndexAs.Builder indexAs = IndexAs.newBuilder().setIndexFieldName(FIELD);
		if (encoding != null) {
			VectorIndexingConfig.Builder vectorIndexingConfig = VectorIndexingConfig.newBuilder().setEncoding(encoding);
			if (flat) {
				vectorIndexingConfig.setIndexType(VectorIndexingConfig.IndexType.FLAT);
			}
			indexAs.setVectorIndexingConfig(vectorIndexingConfig);
		}
		FieldConfig fieldConfig = FieldConfig.newBuilder().setStoredFieldName(FIELD).setFieldType(FieldConfig.FieldType.VECTOR).addIndexAs(indexAs).build();
		IndexSettings indexSettings = IndexSettings.newBuilder().setIndexName("codecTest").setCreatedIndexVersion(createdIndexVersion).addFieldConfig(fieldConfig).build();
		return new ServerIndexConfig(indexSettings);
	}

	private static String generateIndexAndGetFormatName(ServerIndexConfig config, String field) throws Exception {
		try (Directory directory = new ByteBuffersDirectory()) {
			writeVectors(directory, config);
			try (DirectoryReader reader = DirectoryReader.open(directory)) {
				LeafReaderContext leaf = reader.leaves().getFirst();
				FieldInfo fieldInfo = leaf.reader().getFieldInfos().fieldInfo(field);
				return fieldInfo.getAttribute(PerFieldKnnVectorsFormat.PER_FIELD_FORMAT_KEY);
			}
		}
	}

	private static long generateIndexAndGetSize(ServerIndexConfig config) throws Exception {
		try (Directory directory = new ByteBuffersDirectory()) {
			writeVectors(directory, config);
			long total = 0;
			for (String file : directory.listAll()) {
				total += directory.fileLength(file);
			}
			return total;
		}
	}

	private static void writeVectors(Directory directory, ServerIndexConfig config, String... fields) throws Exception {
		if (fields.length == 0) {
			fields = new String[] { FIELD };
		}
		IndexWriterConfig writerConfig = new IndexWriterConfig(new StandardAnalyzer());
		writerConfig.setCodec(new ZuliaLucene104Codec(config));
		try (IndexWriter writer = new IndexWriter(directory, writerConfig)) {
			Random random = new Random(42); // deterministic so comparable across runs
			for (int i = 0; i < DOCS; i++) {
				float[] vector = generateNonZeroVector(random);
				Document document = new Document();
				for (String field : fields) {
					document.add(new KnnFloatVectorField(field, vector, VectorSimilarityFunction.COSINE));
				}
				writer.addDocument(document);
			}
			writer.forceMerge(1); // single segment means one leaf, stable format attributes
		}
	}

	private static float @NonNull [] generateNonZeroVector(Random random) {
		float[] vector = new float[DIMS];
		for (int d = 0; d < DIMS; d++) {
			vector[d] = random.nextFloat() * 2f - 1f;
		}
		vector[0] += 1f; // guarantee a non-zero vector for COSINE
		return vector;
	}
}
