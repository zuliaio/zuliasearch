package io.zulia.util;

import com.google.protobuf.ByteString;
import com.j256.simplemagic.ContentInfo;
import com.j256.simplemagic.ContentInfoUtil;
import org.bson.BsonBinaryReader;
import org.bson.BsonBinaryWriter;
import org.bson.Document;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.io.BasicOutputBuffer;
import org.bson.json.JsonMode;
import org.bson.json.JsonWriterSettings;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class ZuliaUtil {

	private static CodecRegistry pojoCodecRegistry;

	public static void handleListsUniqueValues(Object o, Consumer<? super Object> action) {
		handleListsUniqueValues(o, action, new AtomicInteger(), new AtomicInteger());
	}

	public static void handleListsUniqueValues(Object o, Consumer<? super Object> action, AtomicInteger listSize, AtomicInteger setSize) {
		Set<Object> objects = new LinkedHashSet<>();
		handleLists(o, objects::add, listSize);

		for (Object object : objects) {
			setSize.incrementAndGet();
			action.accept(object);
		}
	}

	public static void handleLists(Object o, Consumer<? super Object> action) {
		handleLists(o, action, new AtomicInteger());
	}

	public static void handleLists(Object o, Consumer<? super Object> action, AtomicInteger listSize) {
		if (o instanceof Collection) {
			Collection<?> c = (Collection<?>) o;
			c.stream().filter(Objects::nonNull).forEach(obj -> {
				if (obj instanceof Collection) {
					handleLists(obj, action, listSize);
				}
				else {
					listSize.incrementAndGet();
					action.accept(obj);
				}
			});
		}
		else if (o instanceof Object[]) {
			Object[] arr = (Object[]) o;
			for (Object obj : arr) {
				if (obj != null) {
					listSize.incrementAndGet();
					action.accept(action);
				}
			}
		}
		else {
			if (o != null) {
				listSize.incrementAndGet();
				action.accept(o);
			}
		}
	}

	public static byte[] mongoDocumentToByteArray(Document mongoDocument) {
		BasicOutputBuffer outputBuffer = new BasicOutputBuffer();
		BsonBinaryWriter writer = new BsonBinaryWriter(outputBuffer);

		EncoderContext encoderContext = EncoderContext.builder().isEncodingCollectibleDocument(true).build();
		if (pojoCodecRegistry != null) {
			new DocumentCodec(pojoCodecRegistry).encode(writer, mongoDocument, encoderContext);
		}
		else {
			new DocumentCodec().encode(writer, mongoDocument, encoderContext);
		}
		return outputBuffer.toByteArray();
	}

	public static ByteString mongoDocumentToByteString(Document mongoDocument) {
		return ByteString.copyFrom(mongoDocumentToByteArray(mongoDocument));
	}

	public static Document byteStringToMongoDocument(ByteString bytes) {
		if (bytes != null) {
			return byteArrayToMongoDocument(bytes.toByteArray());
		}
		return new Document();
	}

	public static Document byteArrayToMongoDocument(byte[] byteArray) {
		if (byteArray != null && byteArray.length != 0) {
			BsonBinaryReader bsonReader = new BsonBinaryReader(ByteBuffer.wrap(byteArray));
			DecoderContext decoderContext = DecoderContext.builder().build();
			if (pojoCodecRegistry != null) {
				return new DocumentCodec(pojoCodecRegistry).decode(bsonReader, decoderContext);
			}
			else {
				return new DocumentCodec().decode(bsonReader, decoderContext);
			}
		}
		return new Document();
	}

	public static int computeLevenshteinDistance(String string1, String string2) {

		char str1[] = string1.toCharArray();
		char str2[] = string2.toCharArray();

		int insert = 40;
		int delete = 40;
		int substitute = 40;
		int substituteCase = 1;

		if (string1.length() < string2.length()) {
			insert = 2;
		}
		int distance[][] = new int[str1.length + 1][str2.length + 1];

		for (int i = 0; i <= str1.length; i++) {
			distance[i][0] = i * delete;
		}
		for (int j = 0; j <= str2.length; j++) {
			distance[0][j] = j * insert;
		}

		for (int i = 1; i <= str1.length; i++) {
			for (int j = 1; j <= str2.length; j++) {
				boolean same = (str1[i - 1] == str2[j - 1]);

				int subPen = 0;

				if (!same) {
					if (Character.toLowerCase(str1[i - 1]) == Character.toLowerCase(str2[j - 1])) {
						subPen = substituteCase;
					}
					else {
						subPen = substitute;
					}
				}

				distance[i][j] = minimum(distance[i - 1][j] + delete, distance[i][j - 1] + insert, distance[i - 1][j - 1] + subPen);
			}
		}
		return distance[str1.length][str2.length];
	}

	private static int minimum(int a, int b, int c) {
		if (a <= b && a <= c)
			return a;
		if (b <= a && b <= c)
			return b;
		return c;
	}

	public static void setPojoCodecRegistry(CodecRegistry pojoCodecRegistry) {
		ZuliaUtil.pojoCodecRegistry = pojoCodecRegistry;
	}

	public static CodecRegistry getPojoCodecRegistry() {
		return pojoCodecRegistry;
	}

	public static String guessExtension(File file) {
		ContentInfoUtil util = new ContentInfoUtil();
		try {
			ContentInfo info = util.findMatch(file);
			return info.getContentType().getMimeType();
		}
		catch (Exception e) {
			return "";
		}
	}

	public static String guessExtension(byte[] bytes) {
		ContentInfoUtil util = new ContentInfoUtil();
		try {
			ContentInfo info = util.findMatch(bytes);
			return info.getContentType().getMimeType();
		}
		catch (Exception e) {
			return "";
		}
	}

	public static Document jsonToMongoDocument(String json) {
		return Document.parse(json);
	}


	public static String mongoDocumentToJson(Document document) {
		return mongoDocumentToJson(document, false);
	}
	public static String mongoDocumentToJson(Document document, boolean pretty) {
		if (pretty) {
			return document.toJson(JsonWriterSettings.builder().indent(true).outputMode(JsonMode.RELAXED).build());
		}
		return document.toJson();
	}
}
